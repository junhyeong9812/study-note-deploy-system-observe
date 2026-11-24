package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.metric.RollupOutcome
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RuleState
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 실제 PostgreSQL — 명세 §16 · §16.1 (리뷰 루프 2): 대상마다 줄의 머리(한 대상의 알림이 많아도 다른 대상이 간다) · 다시 전달된 항목은 수신으로 치지 않음 ·
 * `latest` 의 서버 목록(마지막 수신 기록이 있는 서버) · 5분 평균은 구간마다 따로(실패 구간이 다른 구간을 되돌리지 않음 · 한 바퀴 200 구간 · 차례) ·
 * 두 자리 동시 평균 · 표 만들기는 한 줄로 · 판정의 읽기는 판정용 연결. 표는 전용 스키마(`review_two`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReviewTwoEngineTest {
    private val schema = "review_two"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()

    private fun newPool(size: Int, connectionTimeoutMs: Long = 30_000) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@ReviewTwoEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
        connectionTimeout = connectionTimeoutMs
    })

    /** 그 사례가 끝나면 닫히는 풀 */
    private fun pool(size: Int, connectionTimeoutMs: Long = 30_000) = newPool(size, connectionTimeoutMs).also { testPools += it }

    private val dataSource = newPool(6)
    private val store = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val thresholdStore = ThresholdStore(JdbcTemplate(dataSource))
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        thresholdStore.createSchema()
    }

    @AfterEach fun closeTestPools() { testPools.forEach { it.close() }; testPools.clear() }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private val probe = ContainerValues("probe-app", "0000000000aa", "study-note-deploy-system-probe", null, null, null, "running")

    private fun sample(measuredAt: Instant, receivedAt: Instant = measuredAt.plusSeconds(1), server: String = "ci-agent-158", cpu: Double? = 10.0) =
        ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), server, measuredAt, receivedAt, 30_000,
            HostValues(cpuPct = cpu, load1 = 0.1), listOf(probe), emptyList(), emptyList())

    private fun rawAt(at: Instant, server: String = "ci-agent-158") = store.containers(server, at, at).isNotEmpty()

    // ── 1(a) 대상마다 줄의 머리 ───────────────────────────

    @Test
    fun `한 대상의 알림 60건이 모두 다시 시도여도 첫 바퀴에 다른 대상의 알림이 전달된다`() {
        fun alert(id: String, server: String, at: Instant) =
            ResourceAlert(id, "resource:$server:memory", "resource_memory", AlertSeverity.WARNING, "t", "d", server, ResourceRule.MEMORY, null, at)
        val stuck = (1..60).map { alert("stuck-$it", "ci-agent-158", now.plusMillis(it.toLong())) }
        val other = alert("other", "ci-agent-164", now.plusSeconds(10))
        thresholdStore.judgeRound(now) { JudgeResult(emptyList<RuleState>(), stuck + other) }
        val sent = mutableListOf<String>()
        val sender = object : AlertSender {
            override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return if (alert.server == "ci-agent-158") SendOutcome.Retry("http 503") else SendOutcome.Delivered }
        }
        AlertDispatcher(thresholdStore, sender).dispatch(now.plusSeconds(20))
        assertTrue("other" in sent, "오래된 50건만 읽어 다른 대상의 알림을 영영 못 보낸다: 보낸 것 ${sent.size}건")
        assertEquals(1, sent.count { it.startsWith("stuck") }, "다시 시도가 된 대상의 다음 건을 같은 바퀴에 보냈다")
        assertEquals(60, thresholdStore.pendingAlerts(100).size)
    }

    // ── 2 다시 전달된 항목 ───────────────────────────────

    @Test
    fun `같은 스트림 ID 가 수신 시각만 바뀌어 다시 와도 저장 0건이고 마지막 수신 시각은 그대로다`() {
        val first = sample(now.minusSeconds(60), receivedAt = now.minusSeconds(59))
        store.insertAll(listOf(first))
        assertEquals(0, store.insertAll(listOf(first.copy(receivedAt = now))))
        assertEquals(now.minusSeconds(59), store.lastReceivedOfEveryServer()["ci-agent-158"], "다시 전달된 항목을 새 수신으로 쳤다")
    }

    @Test
    fun `새 항목과 다시 전달된 항목이 한 묶음이면 새 항목의 서버만 오른다`() {
        val old = sample(now.minusSeconds(60), receivedAt = now.minusSeconds(59), server = "ci-agent-158")
        store.insertAll(listOf(old))
        val fresh = sample(now.minusSeconds(5), receivedAt = now, server = "ci-agent-164")
        assertEquals(1, store.insertAll(listOf(old.copy(receivedAt = now), fresh)))
        assertEquals(mapOf("ci-agent-158" to now.minusSeconds(59), "ci-agent-164" to now), store.lastReceivedOfEveryServer())
    }

    // ── 3 latest 의 서버 목록 ────────────────────────────

    @Suppress("UNCHECKED_CAST")
    private fun latestServers() = (((MetricController(store, "secret", "").latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }, now)
        .body as Map<String, Any?>)["data"] as Map<String, Any?>)["servers"] as List<Map<String, Any?>>).associateBy { it["server"] }

    @Test
    fun `기대 서버가 아닌 서버가 25시간 전에 마지막으로 보냈으면 수신 없음으로 나오고 마지막 수신 시각은 채워진다 — 원본이 지워진 뒤에도`() {
        val lastSeen = now.minus(Duration.ofHours(25))
        store.insertAll(listOf(sample(lastSeen, receivedAt = lastSeen, server = "ci-agent-9")))
        fun check(label: String) {
            val server = latestServers()["ci-agent-9"] ?: fail("$label: 마지막 수신 기록이 있는 서버가 목록에서 빠졌다")
            assertEquals(lastSeen.toString(), server["received_at"], label)
            assertEquals(true, server["stale"], label)
            assertNull(server["host"], label)
        }
        check("25시간 전")
        store.insertAll(listOf(sample(now.minus(Duration.ofDays(8)), receivedAt = now.minus(Duration.ofDays(8)), server = "ci-agent-1")))
        store.rollUp(now)
        store.deleteExpired(now)
        assertNotNull(latestServers()["ci-agent-1"], "원본이 7일 지나 지워진 서버가 목록에서 빠졌다")
    }

    // ── 7 · 8 5분 평균은 구간마다 따로 ─────────────────────

    /** 평균을 내다 넘쳐 계산에 실패하는 구간 — 파서를 거치지 않고 저장소에 직접 넣는다(파서는 상한으로 막는다) */
    private fun overflowing(bucket: Instant, server: String = "ci-agent-158") =
        listOf(sample(bucket.plusSeconds(10), server = server, cpu = 1e308), sample(bucket.plusSeconds(40), server = server, cpu = 1e308))

    @Test
    fun `한 구간의 계산 실패가 같은 바퀴의 다른 서버 · 다른 구간을 되돌리지 않고 실패 구간에는 계산함 표시가 없다`() {
        val good = now.minus(Duration.ofDays(8))
        val failing = good.plus(Duration.ofMinutes(5))
        store.insertAll(listOf(sample(good.plusSeconds(10)), sample(good.plusSeconds(20), server = "ci-agent-164")) + overflowing(failing))
        val outcome = store.rollUpBuckets(now)
        assertEquals(RollupOutcome(2, 1), outcome, "실패 구간이 바퀴 전체를 되돌렸거나 세지 않았다")
        assertEquals(1, store.rollups("ci-agent-158", MetricScope.HOST, null, good, good.plus(Duration.ofMinutes(5))).size)
        assertEquals(1, store.rollups("ci-agent-164", MetricScope.HOST, null, good, good.plus(Duration.ofMinutes(5))).size)
        assertEquals(emptyList<Any>(), store.rollups("ci-agent-158", MetricScope.HOST, null, failing, failing.plus(Duration.ofMinutes(5))))
        store.deleteExpired(now)
        assertFalse(rawAt(good.plusSeconds(10)), "평균이 만들어진 다른 구간의 8일 전 원본이 지워지지 않았다")
        assertFalse(rawAt(good.plusSeconds(20), "ci-agent-164"))
        assertTrue(rawAt(failing.plusSeconds(10)), "계산하지 못한 구간의 원본을 지웠다")
    }

    private fun bucketsBack(count: Int, from: Int = 1) = (from until from + count).map { now.minus(Duration.ofMinutes(5L * it)) }

    @Test
    fun `밀린 구간 450개는 세 바퀴에 200 · 200 · 50`() {
        store.insertAll(bucketsBack(450).map { sample(it.plusSeconds(10)) })
        assertEquals(listOf(RollupOutcome(200, 0), RollupOutcome(200, 0), RollupOutcome(50, 0), RollupOutcome(0, 0)),
            (1..4).map { store.rollUpBuckets(now) })
    }

    @Test
    fun `계속 실패하는 구간이 앞자리를 차지하지 않아 나머지 구간이 전부 계산된다`() {
        val failing = bucketsBack(200, from = 100)                                    // 오래된 쪽 200 구간이 늘 실패
        val good = bucketsBack(50)                                                    // 최근 50 구간
        store.insertAll(failing.flatMap { overflowing(it) } + good.map { sample(it.plusSeconds(10)) })
        val rounds = (1..2).map { store.rollUpBuckets(now) }
        assertTrue(rounds.all { it.computed + it.failed <= 200 }, "한 바퀴가 200 구간을 넘었다: $rounds")
        assertEquals(50, rounds.sumOf { it.computed }, "실패 구간이 앞자리를 차지해 뒤 구간을 막았다: $rounds")
    }

    // ── 9 두 자리 동시 평균 ──────────────────────────────

    @Test
    fun `두 자리가 동시에 여러 번 평균을 내도 예외가 없고 결과가 한 번 돌린 것과 같다`() {
        val servers = (1..10).map { "ci-agent-$it" }
        val buckets = bucketsBack(20)
        store.insertAll(servers.flatMap { server -> buckets.flatMap { listOf(sample(it.plusSeconds(10), server = server, cpu = 10.0), sample(it.plusSeconds(40), server = server, cpu = 30.0)) } })
        val other = JdbcTemplate(pool(4)).let { MetricStore(it, it, it) }
        val failures = ConcurrentHashMap.newKeySet<String>()
        val workers = listOf(store, other).flatMap { candidate ->
            (1..2).map { thread(name = "observe-test-rollup") { repeat(5) { round ->
                runCatching { candidate.rollUp(now.plusMillis(round.toLong())) }.onFailure { failures += "${it.javaClass.simpleName}: ${it.message?.take(120)}" }
            } } }
        }
        workers.forEach { it.join(60_000) }
        assertEquals(emptySet<String>(), failures, "동시 평균이 실패했다(교착 등)")
        for (server in servers) for (bucket in buckets) {
            assertEquals(xyz.junproject.observe.metric.MetricAggregate(20.0, 30.0, 2),
                store.rollups(server, MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5))).single().values["cpu_pct"], "$server $bucket")
        }
    }

    // ── 10 표 만들기는 한 줄로 ───────────────────────────

    @Test
    fun `빈 스키마에서 수집 한 바퀴와 미리 만들기가 동시에 돌아도 수집이 실패하지 않는다 — 20번`() {
        val failures = mutableListOf<String>()
        repeat(20) { round ->
            administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
            administrationJdbc.execute("CREATE SCHEMA $schema")
            val fresh = JdbcTemplate(pool(4)).let { MetricStore(it, it, it) }                 // 새 프로세스 — 아직 표를 만들지 않았다
            val stream = FakeMetricStream().also { it.add(MetricRecords.resource("${now.toEpochMilli() + round}-0", now.minusSeconds(1))) }
            val collector = MetricCollector(stream, fresh, block = Duration.ZERO, clock = { now })
            val preparing = thread { runCatching { fresh.prepareSchema() } }
            val collecting = thread { collector.runOnce() }
            preparing.join(30_000); collecting.join(30_000)
            collector.lastFailure?.let { failures += "$round: $it" }
            closeTestPools()
        }
        assertEquals(emptyList<String>(), failures, "표 만들기가 겹쳐 첫 수집이 실패했다")
    }

    // ── 11 판정의 읽기는 판정용 연결 ──────────────────────

    @Test
    fun `조회 풀의 연결을 전부 쥔 상태에서도 판정 한 바퀴가 돈다`() {
        val queryPool = pool(1, connectionTimeoutMs = 1_000)
        val judgePool = pool(2)
        val metrics = MetricStore(JdbcTemplate(dataSource), JdbcTemplate(queryPool), JdbcTemplate(dataSource), JdbcTemplate(judgePool))
        metrics.insertAll(listOf(sample(now.minusSeconds(30)), sample(now.minusSeconds(5))))
        val held: Connection = queryPool.connection                                       // 어드민 조회가 몰려 조회 풀이 다 잡혔다
        try {
            val sender = object : AlertSender { override fun send(alert: ResourceAlert) = SendOutcome.Delivered }
            val result = ThresholdRound(metrics, thresholdStore, ThresholdJudge(), AlertDispatcher(thresholdStore, sender), thresholdStore, emptySet()).judgeOnce(now)
            assertTrue(result.judged, "${result.judgeFailure}")
            assertNull(result.judgeFailure, "판정이 조회 풀을 기다리다 실패했다")
        } finally {
            held.close()
        }
    }
}
