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
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricAggregate
import xyz.junproject.observe.metric.MetricDeletion
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 실제 PostgreSQL — 5분 평균과 저장이 엇갈릴 때 원본을 잃지 않는 장치(명세 §11.3 · 리뷰 루프 1 의 지적 반증용, 지금은 초록이어야 한다):
 * ① 수신 시각은 평균 계산 시각보다 앞인데 평균을 낸 뒤에 저장된 원본 — 다음 평균이 그 구간을 다시 계산하고, 그 전의 삭제는 그 구간을 지우지 않는다.
 * ② 저장 트랜잭션이 열려 있는 동안 평균을 돌리면 평균은 그 저장을 기다렸다가 포함해 계산한다.
 * 저장이 진행 중인 상태는 시험용 트리거(원본 표 `metric_sample` 의 BEFORE INSERT 가 권고 잠금을 기다림)로 만든다. 표는 전용 스키마(`metric_concurrency`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricConcurrencyEngineTest {
    private val schema = "metric_concurrency"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 3)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@MetricConcurrencyEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val store = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private fun sample(measuredAt: Instant, receivedAt: Instant, cpu: Double) =
        ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), "ci-agent-158", measuredAt, receivedAt, 30_000,
            HostValues(cpuPct = cpu), emptyList(), emptyList(), emptyList())

    private fun cpuOf(bucket: Instant) =
        store.rollups("ci-agent-158", MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5))).single().values["cpu_pct"]

    @Test
    fun `평균을 낸 뒤 저장된 원본은 수신 시각이 계산 시각보다 앞이어도 다음 평균이 다시 계산하고 그 전의 삭제는 그 구간을 지우지 않는다`() {
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
        store.rollUp(now)
        // 다른 자리가 평균 계산(now) 전에 받아 두었다가 계산 뒤에 커밋한 원본
        store.insertAll(listOf(sample(bucket.plusSeconds(40), now.minusSeconds(10), 30.0)))
        assertEquals(MetricDeletion(0, 0), store.deleteExpired(now.plusSeconds(1)), "다시 계산하기 전에 그 구간을 지웠다 — 늦게 저장된 원본이 평균에 들지 못한다")
        store.rollUp(now.plusSeconds(2))
        assertEquals(MetricAggregate(20.0, 30.0, 2), cpuOf(bucket), "늦게 저장된 원본을 다시 계산하지 않았다")
        assertEquals(MetricDeletion(2, 0), store.deleteExpired(now.plusSeconds(3)))
    }

    @Test
    fun `저장 트랜잭션이 열려 있는 동안 돈 평균은 그 저장을 기다렸다가 포함해 계산한다`() {
        val bucket = now.minus(Duration.ofMinutes(20))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
        administrationJdbc.execute("""
            CREATE FUNCTION $schema.observe_test_gate() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN PERFORM pg_advisory_xact_lock_shared(4242); RETURN NEW; END ${'$'}${'$'}""")
        administrationJdbc.execute("CREATE TRIGGER observe_test_gate BEFORE INSERT ON $schema.metric_sample FOR EACH ROW EXECUTE FUNCTION $schema.observe_test_gate()")

        val gate: Connection = administration.connection
        gate.createStatement().use { it.execute("SELECT pg_advisory_lock(4242)") }           // 저장이 트리거에서 멈춘다(트랜잭션은 열린 채)
        var insertFailure: Throwable? = null
        var rollUpFailure: Throwable? = null
        try {
            val inserting = thread(name = "observe-test-insert") {
                runCatching { store.insertAll(listOf(sample(bucket.plusSeconds(40), bucket.plusSeconds(41), 30.0))) }.onFailure { insertFailure = it }
            }
            waitUntil("저장이 트리거에서 기다리기") {
                administrationJdbc.queryForObject("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted", Long::class.java)!! > 0
            }
            val rolling = thread(name = "observe-test-rollup") { runCatching { store.rollUp(now) }.onFailure { rollUpFailure = it } }
            rolling.join(1_500)
            assertTrue(rolling.isAlive, "평균이 열린 저장을 기다리지 않고 끝났다 — 그 원본 없이 계산했다")
            gate.createStatement().use { it.execute("SELECT pg_advisory_unlock(4242)") }
            inserting.join(10_000)
            rolling.join(10_000)
        } finally {
            runCatching { gate.createStatement().use { it.execute("SELECT pg_advisory_unlock_all()") } }
            gate.close()
        }
        assertNull(insertFailure)
        assertNull(rollUpFailure)
        assertEquals(MetricAggregate(20.0, 30.0, 2), cpuOf(bucket), "기다린 평균이 그 저장을 포함하지 않았다")
    }

    private fun waitUntil(what: String, condition: () -> Boolean) {
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (System.nanoTime() < deadline) { if (condition()) return; Thread.sleep(20) }
        fail<Unit>("$what — 10초 안에 되지 않았다")
    }
}
