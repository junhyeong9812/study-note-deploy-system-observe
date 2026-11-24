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
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricAggregate
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricDeletion
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.AlertTarget
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RuleState
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdStore
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 PostgreSQL — 명세 §16.2 (리뷰 루프 3): `firing` 의 읽기는 조회용 연결로(표 확인 길 포함) · 대상의 차례를 실제 표로(대상 시각 · 디스크 묶음) ·
 * 되돌려진 저장이 저장 순번을 써 버린 뒤에도 "다시 계산" · "삭제 보류"가 선다. 표는 전용 스키마(`review_three`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReviewThreeEngineTest {
    private val schema = "review_three"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()

    private fun newPool(size: Int, connectionTimeoutMs: Long = 30_000) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@ReviewThreeEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
        connectionTimeout = connectionTimeoutMs
    })

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

    private fun alert(eventId: String, server: String, rule: ResourceRule = ResourceRule.MEMORY, key: String = "resource:$server:${rule.wireName}") =
        ResourceAlert(eventId, key, rule.kind, AlertSeverity.WARNING, "t", "d", server, rule, null, now.plusMillis(sequence.incrementAndGet().toLong()))

    // ── firing 의 읽기는 조회용 연결로 ────────────────────

    @Test
    fun `판정 · 알림 풀의 연결을 전부 쥔 상태에서도 firing 과 latest 가 답한다 — 표 확인 길 포함`() {
        thresholdStore.judgeRound(now) {
            JudgeResult(listOf(RuleState("ci-agent-158", ResourceRule.MEMORY, "", null, true, now, now, 93.0)), emptyList())
        }
        val judgePool = pool(1, connectionTimeoutMs = 1_000)
        val queryPool = pool(2)
        val fresh = ThresholdStore(JdbcTemplate(judgePool), query = JdbcTemplate(queryPool))     // 새 자리 — 아직 표를 확인하지 않았다
        val held: Connection = judgePool.connection                                               // 판정 · 보내기가 연결을 다 잡았다
        try {
            assertEquals(listOf("ci-agent-158" to "memory"), fresh.firing().map { it.server to it.rule }, "firing 이 판정 · 알림 풀을 기다렸다")
            val response = MetricController(store, "secret", "", fresh).latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }, now)
            assertEquals(200, response.statusCode.value(), "${response.body}")
            @Suppress("UNCHECKED_CAST")
            val firing = ((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["firing"] as List<Map<String, Any?>>
            assertEquals(listOf("memory"), firing.map { it["rule"] })
        } finally {
            held.close()
        }
    }

    // ── 대상의 차례를 실제 표로 ──────────────────────────

    @Test
    fun `실제 보낼 표에서 대상 다섯이 10초씩 다시 시도면 둘째 바퀴는 target-4 target-5 target-1`() {
        thresholdStore.judgeRound(now) { JudgeResult(emptyList(), (1..5).map { alert("target-$it", "server-$it") }) }
        var clock = now.plusSeconds(60)
        val sent = mutableListOf<String>()
        val sender = object : AlertSender {
            override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; clock = clock.plusSeconds(10); return SendOutcome.Retry("http 503") }
        }
        val rounds = (0 until 3).map {
            val before = sent.size
            AlertDispatcher(thresholdStore, sender, clock = { clock }).dispatch(clock)
            clock = clock.plusSeconds(30)
            sent.drop(before)
        }
        assertEquals(listOf("target-1", "target-2", "target-3"), rounds[0])
        assertEquals(listOf("target-4", "target-5", "target-1"), rounds[1], "표가 대상의 마지막 시도 시각을 남기거나 돌려주지 않는다")
        assertEquals(listOf("target-2", "target-3", "target-4"), rounds[2])
    }

    @Test
    fun `실제 보낼 표에서 디스크 경고 · 위험의 알림은 한 대상 줄로 묶여 나온다`() {
        thresholdStore.judgeRound(now) {
            JudgeResult(emptyList(), listOf(
                alert("critical", "ci-agent-158", ResourceRule.DISK_CRITICAL),
                alert("warning", "ci-agent-158", ResourceRule.DISK_WARNING, key = "resource:ci-agent-158:disk_warning"),
                alert("memory", "ci-agent-158"),
            ))
        }
        val queues = thresholdStore.pendingByTarget(10).associateBy { it.target }
        assertEquals(setOf(AlertTarget("ci-agent-158", "disk", null), AlertTarget("ci-agent-158", "memory", null)), queues.keys)
        assertEquals(listOf("critical", "warning"), queues.getValue(AlertTarget("ci-agent-158", "disk", null)).alerts.map { it.alert.eventId })
        // 대상마다 한 건이면 디스크 줄의 머리 하나만 — 규칙마다 따로 머리를 고르면 둘이 나온다
        assertEquals(listOf("critical"), thresholdStore.pendingByTarget(1).single { it.target.ruleGroup == "disk" }.alerts.map { it.alert.eventId })
        // 위험에 표시한 시도 시각이 디스크 대상의 시각으로 돌아온다
        val criticalId = thresholdStore.pendingByTarget(1).single { it.target.ruleGroup == "disk" }.alerts.single().id
        thresholdStore.markRetry(criticalId, now.plusSeconds(5), "http 503")
        assertEquals(now.plusSeconds(5), thresholdStore.pendingByTarget(1).single { it.target.ruleGroup == "disk" }.lastAttemptAt)
    }

    // ── 되돌려진 저장이 저장 순번을 써 버린 뒤 ───────────

    private fun sample(measuredAt: Instant, receivedAt: Instant, cpu: Double, containers: List<ContainerValues> = emptyList()) =
        ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), "ci-agent-158", measuredAt, receivedAt, 30_000,
            HostValues(cpuPct = cpu), containers, emptyList(), emptyList())

    @Test
    fun `되돌려진 저장이 순번을 써 버린 뒤에도 평균 뒤에 저장된 원본은 다시 계산되고 그 전의 삭제는 그 구간을 지우지 않는다`() {
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
        val broken = ContainerValues("bad\u0000name", "0123456789ab", "study-note-deploy-system-front", 1.0, 1, 0, "running")
        assertThrows(Exception::class.java) {                                                   // 원본 행은 순번을 받은 뒤 같은 트랜잭션의 컨테이너 행에서 실패해 되돌려진다
            store.insertAll(listOf(sample(bucket.plusSeconds(20), now.minusSeconds(30), 50.0), sample(bucket.plusSeconds(25), now.minusSeconds(30), 70.0, listOf(broken))))
        }
        store.rollUp(now)
        store.insertAll(listOf(sample(bucket.plusSeconds(40), now.minusSeconds(10), 30.0)))     // 평균 뒤에 저장 — 수신은 계산 시각보다 앞
        assertEquals(MetricDeletion(0, 0), store.deleteExpired(now.plusSeconds(1)), "다시 계산하기 전에 그 구간을 지웠다")
        store.rollUp(now.plusSeconds(2))
        assertEquals(MetricAggregate(20.0, 30.0, 2),
            store.rollups("ci-agent-158", MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5))).single().values["cpu_pct"])
        assertEquals(MetricDeletion(2, 0), store.deleteExpired(now.plusSeconds(3)))
    }
}
