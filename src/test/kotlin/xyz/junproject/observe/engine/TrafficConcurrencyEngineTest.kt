package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.traffic.TrafficCellQuery
import xyz.junproject.observe.traffic.TrafficRecords
import xyz.junproject.observe.traffic.TrafficStore
import xyz.junproject.observe.traffic.TrafficStoreOutcome
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

/**
 * 실제 PostgreSQL — 규약 4.5 "두 자리(blue · green)가 함께 받는 때": 두 연결이 같은 항목을 동시에 넣어도 한 번만 들어가고,
 * 순번의 판정은 트랜잭션의 첫 문장(열쇠의 `INSERT … ON CONFLICT DO NOTHING`)이 한다 — "있는지 확인한 뒤 쓰기"면 두 자리가 함께 칸을 넣는다.
 * 저장이 칸을 넣는 중인 상태는 시험용 트리거(원본 칸 표 `traffic_segment` 의 BEFORE INSERT 가 권고 잠금을 기다림)로 만든다. 표는 전용 스키마.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TrafficConcurrencyEngineTest {
    private val schema = "traffic_concurrency"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 3)
    private val administrationJdbc = JdbcTemplate(administration)
    /** 두 자리 — 연결 풀도 따로 */
    private fun place() = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@TrafficConcurrencyEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 3
    })
    private val blueSource = place()
    private val greenSource = place()
    private val blue = JdbcTemplate(blueSource).let { TrafficStore(it, it, it) }
    private val green = JdbcTemplate(greenSource).let { TrafficStore(it, it, it) }
    private val now = Instant.parse("2026-11-25T04:00:00Z")
    private val cell = now.minusSeconds(30)

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        blue.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        blueSource.close(); greenSource.close(); administration.close()
    }

    private fun waitingOnGate(): Long =
        administrationJdbc.queryForObject("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 4343 AND NOT granted", Long::class.java)!!

    private fun waitUntil(what: String, condition: () -> Boolean) {
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (System.nanoTime() < deadline) { if (condition()) return; Thread.sleep(20) }
        fail<Unit>("$what — 10초 안에 되지 않았다")
    }

    @Test
    fun `두 자리가 같은 항목을 동시에 넣으면 한 번만 들어가고 다른 자리는 순번으로 버린다 — 칸을 넣기 전에 열쇠에서 기다린다`() {
        administrationJdbc.execute("""
            CREATE FUNCTION $schema.observe_test_gate() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN PERFORM pg_advisory_xact_lock_shared(4343); RETURN NEW; END ${'$'}${'$'}""")
        administrationJdbc.execute("CREATE TRIGGER observe_test_gate BEFORE INSERT ON $schema.traffic_segment FOR EACH ROW EXECUTE FUNCTION $schema.observe_test_gate()")
        val entryBody = TrafficRecords.body(listOf(TrafficRecords.segment(cell, listOf(200, 200, 500))), listOf(TrafficRecords.request(cell)))
        val gate: Connection = administration.connection
        gate.createStatement().use { it.execute("SELECT pg_advisory_lock(4343)") }
        val outcomes = ConcurrentLinkedQueue<TrafficStoreOutcome>()
        val failures = ConcurrentLinkedQueue<Throwable>()
        val start = CountDownLatch(1)
        try {
            val places = listOf(blue, green).mapIndexed { index, store ->
                thread(name = "observe-test-place-$index") {
                    start.await()
                    runCatching { store.store(TrafficRecords.entry(1, now.minusSeconds(1), now, entryBody)) }.onSuccess { outcomes += it }.onFailure { failures += it }
                }
            }
            start.countDown()
            waitUntil("한 자리가 칸을 넣다가 문에서 기다리기") { waitingOnGate() >= 1 }
            Thread.sleep(1_000)                                  // 다른 자리도 문까지 올 수 있었다면 왔을 시간
            assertEquals(1L, waitingOnGate(), "두 자리가 모두 칸을 넣으러 왔다 — 열쇠를 먼저 넣지 않았다(확인한 뒤 쓰기)")
            gate.createStatement().use { it.execute("SELECT pg_advisory_unlock(4343)") }
            places.forEach { it.join(10_000) }
        } finally {
            runCatching { gate.createStatement().use { it.execute("SELECT pg_advisory_unlock_all()") } }
            gate.close()
        }
        assertEquals(emptyList<Throwable>(), failures.toList(), "한 자리가 예외로 끝났다 — 같은 열쇠는 오류가 아니라 버림이어야 한다")
        assertEquals(listOf(false, true), outcomes.map { it.duplicate }.sorted())
        val totals = blue.cells(TrafficCellQuery(cell, now, cell)).single().totals
        assertEquals(3L, totals.count, "같은 항목의 칸이 두 번 더해졌다")
        assertEquals(1, blue.requestSpans("req-3f9a0c1b2d4e", 10).size)
    }

    @Test
    fun `두 자리가 한 세대의 순번을 홀짝으로 엇갈려 넣어도 여러 판에서 받은 수 · 처음 본 순번 · 틈 · 진도가 늘 맞다`() {
        val rounds = 15
        val lastSequence = 60L
        val failures = ConcurrentLinkedQueue<Throwable>()
        repeat(rounds) { round ->
            val server = "race-$round"
            val start = CountDownLatch(1)
            val places = listOf(blue to 1L, green to 2L).map { (store, first) ->
                thread(name = "observe-test-race-$round-$first") {
                    start.await()
                    for (sequence in first..lastSequence step 2) runCatching {
                        store.store(TrafficRecords.entry(sequence, now.minusSeconds(lastSequence - sequence), now, server = server))
                    }.onFailure { failures += it }
                }
            }
            start.countDown()
            places.forEach { it.join(30_000) }
        }
        assertEquals(emptyList<Throwable>(), failures.toList())
        val progress = blue.serverProgress().associateBy { it.server }
        val wrong = (0 until rounds).map { "race-$it" }.mapNotNull { server ->
            val state = progress.getValue(server)
            val receivedCount = administrationJdbc.queryForObject("SELECT count(*) FROM $schema.traffic_receipt WHERE server = ?", Long::class.java, server)
            if (state.firstSeqSeen == 1L && state.sequenceGaps == 0L && state.readProgressAt == now && receivedCount == lastSequence) null
            else "$server: first=${state.firstSeqSeen} gaps=${state.sequenceGaps} progress=${state.readProgressAt} receipts=$receivedCount"
        }
        assertEquals(emptyList<String>(), wrong, "엇갈린 판에서 받은 수 · 처음 본 순번 · 틈 · 진도가 어긋났다")
    }

    @Test
    fun `서로 다른 항목이 같은 칸을 동시에 채우면 모두 더해진다`() {
        val sequences = (1L..40L).toList()
        val failures = ConcurrentLinkedQueue<Throwable>()
        val start = CountDownLatch(1)
        val workers = sequences.chunked(10).mapIndexed { index, chunk ->
            thread(name = "observe-test-writer-$index") {
                start.await()
                val store = if (index % 2 == 0) blue else green
                for (sequence in chunk) runCatching {
                    store.store(TrafficRecords.entry(sequence, now.minusSeconds(1), now, TrafficRecords.body(listOf(TrafficRecords.segment(cell, listOf(200, 404))))))
                }.onFailure { failures += it }
            }
        }
        start.countDown()
        workers.forEach { it.join(30_000) }
        assertEquals(emptyList<Throwable>(), failures.toList())
        val totals = blue.cells(TrafficCellQuery(cell, now, cell)).single().totals
        assertEquals(80L, totals.count)
        assertEquals(40L, totals.status.status4xx)
        assertEquals(0L, blue.serverProgress().single().sequenceGaps, "동시에 들어온 순번을 틈으로 셌다")
        assertEquals(now.minusSeconds(1), blue.serverProgress().single().readProgressAt, "동시에 들어온 순번에서 진도가 끝까지 나아가지 않았다")
    }
}
