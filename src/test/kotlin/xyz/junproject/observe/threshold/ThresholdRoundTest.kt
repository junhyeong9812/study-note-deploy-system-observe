package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricReader
import xyz.junproject.observe.metric.MetricRollup
import xyz.junproject.observe.metric.MetricSampleSource
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.metric.SeenContainer
import xyz.junproject.observe.metric.SeriesPoint
import xyz.junproject.observe.metric.SeriesQuery
import java.time.Duration
import java.time.Instant

/**
 * 명세 §11.4 의 한 바퀴 — 표본 읽기 → 판정(잠금 · 한 트랜잭션은 저장소) → 보내기. 판정이 실패해도 보내기는 돌고 그 반대도,
 * 예외를 내지 않는다. 그리고 이 바퀴가 헬스 · `firing` · 예약에 닿는 이음매. 저장소 · 보내는 쪽은 Fake.
 */
class ThresholdRoundTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    private class FakeSamples : MetricSampleSource {
        val asked = mutableListOf<Pair<Instant, Instant>>()
        var samples: List<ResourceSample> = emptyList()
        var received: Map<String, Instant> = emptyMap()
        var failure: RuntimeException? = null
        override fun samplesMeasuredBetween(from: Instant, to: Instant): List<ResourceSample> { failure?.let { throw it }; asked += from to to; return samples }
        override fun lastReceivedOfEveryServer(): Map<String, Instant> { failure?.let { throw it }; return received }
    }

    /** 잠금 · 트랜잭션을 흉내 내는 상태 저장소 — 판정 함수가 받은 앞 상태와 돌려준 결과를 남긴다 */
    private class FakeStates : RuleStateStore {
        var states: List<RuleState> = emptyList()
        var lockAvailable = true
        val events = mutableListOf<ResourceAlert>()
        override fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome {
            if (!lockAvailable) return RoundOutcome(false, 0)
            val result = judge(states)
            states = result.states
            events += result.events
            return RoundOutcome(true, result.events.size)
        }
    }

    private class FakeQueue : AlertQueue {
        val pending = mutableListOf<QueuedAlert>()
        var failure: RuntimeException? = null
        var countsFailure: RuntimeException? = null
        override fun pendingAlerts(limit: Int): List<QueuedAlert> { failure?.let { throw it }; return pending.take(limit) }
        override fun markDelivered(id: Long, at: Instant) { pending.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) {}
        override fun markRejected(id: Long, at: Instant, reason: String) { pending.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = block()
        override fun alertCounts(): AlertCounts { countsFailure?.let { throw it }; return AlertCounts(pending.size.toLong(), 2) }
        override fun pendingByTarget(perTarget: Int): List<TargetQueue> { failure?.let { throw it }; return targetQueues(pending, emptyMap(), perTarget) }
    }

    private val samples = FakeSamples()
    private val states = FakeStates()
    private val queue = FakeQueue()
    private val sent = mutableListOf<String>()
    private val sender = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return SendOutcome.Delivered } }
    private val judge = ThresholdJudge()
    private val round = ThresholdRound(samples, states, judge, AlertDispatcher(queue, sender), queue, setOf("ci-agent-164"))

    private val leftover = ResourceAlert("left-1", "resource:ci-agent-9:cpu", "resource_cpu", AlertSeverity.WARNING, "t", "d", "ci-agent-9",
        ResourceRule.CPU, null, now.minusSeconds(60))

    @Test
    fun `한 바퀴는 판정 창만큼의 표본과 기대 서버로 판정하고 결과를 남긴다`() {
        val result = round.runOnce(now)
        assertEquals(listOf(now.minus(judge.lookback) to now), samples.asked)
        assertEquals(listOf("resource:ci-agent-164:silent"), states.events.map { it.key }, "기대 서버가 판정에 닿지 않았다")
        assertEquals(ThresholdRoundResult(now, true, 1, null, DispatchOutcome(true, 0, 0, 0), null), result)
        assertEquals(result, round.lastResult)
        assertEquals(now, round.lastJudgedAt)
    }

    @Test
    fun `표본을 읽지 못해 판정이 실패해도 예외를 내지 않고 앞 바퀴에 남은 알림은 보낸다`() {
        samples.failure = IllegalStateException("DB 불통 — 내부 문구")
        queue.pending += QueuedAlert(1, leftover, 1, "http 503")
        val result = round.runOnce(now)
        assertFalse(result.judged)
        assertEquals("IllegalStateException", result.judgeFailure)
        assertEquals(listOf("left-1"), sent, "판정 실패가 보내기를 막았다")
        assertEquals(DispatchOutcome(true, 1, 0, 0), result.dispatch)
        assertNull(round.lastJudgedAt)
    }

    @Test
    fun `보내기가 실패해도 판정 결과는 남고 예외를 내지 않는다`() {
        queue.failure = IllegalStateException("보낼 표 읽기 실패")
        val result = round.runOnce(now)
        assertTrue(result.judged)
        assertEquals(1, result.eventsCreated)
        assertNull(result.dispatch)
        assertEquals("IllegalStateException", result.dispatchFailure)
        assertEquals(now, round.lastJudgedAt)
    }

    @Test
    fun `다른 자리가 판정 잠금을 쥐고 있으면 판정하지 않았다고 남기고 마지막 판정 시각을 바꾸지 않는다`() {
        round.runOnce(now)
        states.lockAvailable = false
        val result = round.runOnce(now.plusSeconds(30))
        assertFalse(result.judged)
        assertNull(result.judgeFailure, "잠금을 못 얻은 것은 실패가 아니다")
        assertEquals(now, round.lastJudgedAt)
    }

    @Test
    fun `보낼 표의 건수를 읽지 못하면 null`() {
        assertEquals(AlertCounts(0, 2), round.alertCounts())
        queue.countsFailure = IllegalStateException("x")
        assertNull(round.alertCounts())
    }

    // ── 헬스 ─────────────────────────────────────────────

    private var metricsNow = now
    private val metricStream = FakeMetricStream()
    private val metricCollector = MetricCollector(metricStream, FakeMetricSink(), block = Duration.ZERO, clock = { metricsNow })

    private fun metricHealth(thresholds: ThresholdRound?) = MetricHealth(
        metricCollector, { metricStream.unacknowledgedCount() }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null },
        startedAt = now, thresholds = thresholds,
    )

    @Test
    fun `헬스의 지표 칸에 마지막 판정 시각과 못 보낸 알림 수가 나온다`() {
        metricCollector.runOnce()
        val before = metricHealth(round).report(now)
        assertEquals("", before["last_judged_at"])
        assertEquals("", before["last_judge_failure"])
        assertEquals("", before["last_dispatch_failure"])
        queue.pending += QueuedAlert(1, leftover, 3, "http 503")
        queue.failure = null
        val sender503 = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome = SendOutcome.Retry("http 503") }
        val failingRound = ThresholdRound(samples, states, judge, AlertDispatcher(queue, sender503), queue, emptySet())
        failingRound.runOnce(now)
        val after = metricHealth(failingRound).report(now)
        assertEquals(now.toString(), after["last_judged_at"])
        assertEquals(1L, (after["undelivered_alerts"] as Number).toLong())
        assertEquals(2L, (after["rejected_alerts"] as Number).toLong())
        assertEquals("ok", after["status"], "알림을 못 보낸 것이 지표 수집의 판정을 바꿨다")
    }

    @Test
    fun `판정이 실패해도 지표 status 와 로그 헬스의 판정은 그대로이고 실패 종류만 나온다`() {
        metricCollector.runOnce()
        samples.failure = IllegalStateException("x")
        queue.countsFailure = IllegalStateException("y")
        round.runOnce(now)
        val metrics = metricHealth(round).report(now)
        assertEquals("ok", metrics["status"])
        assertEquals("IllegalStateException", metrics["last_judge_failure"])
        assertEquals(-1L, (metrics["undelivered_alerts"] as Number).toLong())
        assertEquals(-1L, (metrics["rejected_alerts"] as Number).toLong())

        val logStream = FakeLogStream()
        val logCollector = LogCollector(logStream, FakeLogSink(), block = Duration.ZERO, clock = { now })
        logCollector.runOnce()
        val report = CollectorHealth(logCollector, { logStream.unacknowledgedCount() }, { 0L }, { null }, { null }, startedAt = now,
            metrics = metricHealth(round)).report(now)
        assertTrue(report.healthy)
        assertEquals("ok", report.body["status"])
    }

    @Test
    fun `판정을 붙이지 않으면 지표 칸에 판정 칸이 없다 — task 03 의 칸 그대로`() {
        metricCollector.runOnce()
        val body = metricHealth(null).report(now)
        for (field in listOf("last_judged_at", "last_judge_failure", "last_dispatch_failure", "undelivered_alerts", "rejected_alerts")) {
            assertFalse(body.containsKey(field), field)
        }
    }

    // ── GET /metrics/latest 의 firing ─────────────────────

    private val emptyReader = object : MetricReader {
        override fun series(query: SeriesQuery): List<SeriesPoint> = emptyList()
        override fun rollups(server: String, scope: MetricScope, name: String?, from: Instant, to: Instant): List<MetricRollup> = emptyList()
        override fun latest(receivedSince: Instant): List<ResourceSample> = listOf(ResourceSample(StreamId(now.toEpochMilli(), 0), "ci-agent-158",
            now, now, 30_000, HostValues(cpuPct = 1.0), emptyList(), emptyList(), emptyList()))
        override fun containers(server: String, from: Instant, to: Instant): List<SeenContainer> = emptyList()
    }

    @Suppress("UNCHECKED_CAST")
    private fun firing(controller: MetricController) =
        ((controller.latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "s") }, now).body as Map<String, Any?>)["data"] as Map<String, Any?>)["firing"]

    @Test
    fun `latest 의 firing 에 지금 넘어 있는 규칙이 명세의 칸 이름으로 나온다`() {
        val source = object : FiringSource {
            override fun firing() = listOf(
                FiringRule("ci-agent-158", "disk_critical", null, "critical", now.minus(Duration.ofMinutes(7)), 96.2),
                FiringRule("ci-agent-158", "restarts", "front-app-green", "warning", now.minusSeconds(90), 4.0),
            )
        }
        assertEquals(listOf(
            mapOf("server" to "ci-agent-158", "rule" to "disk_critical", "target" to null, "severity" to "critical",
                "since" to now.minus(Duration.ofMinutes(7)).toString(), "last_value" to 96.2),
            mapOf("server" to "ci-agent-158", "rule" to "restarts", "target" to "front-app-green", "severity" to "warning",
                "since" to now.minusSeconds(90).toString(), "last_value" to 4.0),
        ), firing(MetricController(emptyReader, "s", "", source)))
    }

    @Test
    fun `넘어 있는 것이 없거나 판정을 붙이지 않았으면 firing 은 빈 배열이다`() {
        assertEquals(emptyList<Any>(), firing(MetricController(emptyReader, "s", "", object : FiringSource { override fun firing() = emptyList<FiringRule>() })))
        assertEquals(emptyList<Any>(), firing(MetricController(emptyReader, "s", "")))
    }

    // ── 예약 ─────────────────────────────────────────────

    @Test
    fun `판정은 설정 키의 간격(기본 30초)으로 예약되어 있다`() {
        val method = ObserveSchedule::class.java.getMethod("judgeThresholds")
        val scheduled = method.getAnnotation(Scheduled::class.java) ?: fail("judgeThresholds() 에 @Scheduled 가 없다")
        assertEquals("\${observe.metrics.judge-interval-ms:30000}", scheduled.fixedDelayString)
    }

    /** 연결을 줄 수 없는 DB 의 로그 저장소 — 예약 객체를 만들 때만 쓴다 */
    private val unreachableLogStore = org.springframework.jdbc.core.JdbcTemplate(object : org.springframework.jdbc.datasource.AbstractDataSource() {
        override fun getConnection(): java.sql.Connection = throw java.sql.SQLException("Connection refused")
        override fun getConnection(username: String?, password: String?): java.sql.Connection = getConnection()
    }).let { xyz.junproject.observe.log.LogStore(it, it, it) }

    @Test
    fun `예약 실행은 그 시각으로 한 바퀴를 돌고 판정을 붙이지 않았으면 null 이다`() {
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore,
            thresholdRound = round)
        assertEquals(now, schedule.judgeThresholds(now)?.ranAt)
        assertEquals(now, round.lastJudgedAt)
        val without = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore)
        assertNull(without.judgeThresholds(now))
    }
}
