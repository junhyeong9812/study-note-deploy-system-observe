package xyz.junproject.observe.threshold

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ClockBehind
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricSampleSource
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant

/**
 * 명세 §15 · §15.1 (리뷰 루프 1) — 판정과 보내기를 나눈다(판정 예약은 보내기를 부르지 않는다 · 보내기 예약은 따로), 표본은 판정 잠금을 얻은 자리만 읽는다,
 * 헬스는 판정 · 보내기용 저장소를 건드리지 않는다(바퀴가 세어 둔 값만), 헬스에 끊김 판정 여부 · 시계가 뒤처진 서버, 수집기의 "따라잡음".
 */
class ThresholdRoundSplitTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    private class CountingSamples : MetricSampleSource {
        var calls = 0
        override fun samplesMeasuredBetween(from: Instant, to: Instant): List<ResourceSample> { calls++; return emptyList() }
        override fun lastReceivedOfEveryServer(): Map<String, Instant> { calls++; return emptyMap() }
    }

    private class FakeStates(var lockAvailable: Boolean = true) : RuleStateStore {
        val events = mutableListOf<ResourceAlert>()
        var states: List<RuleState> = emptyList()
        override fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome {
            if (!lockAvailable) return RoundOutcome(false, 0)
            val result = judge(states); states = result.states; events += result.events
            return RoundOutcome(true, result.events.size)
        }
    }

    /** 모든 호출을 센다 — 헬스 · 판정이 이 면을 건드렸는지 */
    private class CountingQueue : AlertQueue {
        val calls = mutableListOf<String>()
        override fun pendingAlerts(limit: Int): List<QueuedAlert> { calls += "pending"; return emptyList() }
        override fun markDelivered(id: Long, at: Instant) { calls += "delivered" }
        override fun markRetry(id: Long, at: Instant, reason: String) { calls += "retry" }
        override fun markRejected(id: Long, at: Instant, reason: String) { calls += "rejected" }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? { calls += "lock"; return block() }
        override fun alertCounts(): AlertCounts { calls += "counts"; return AlertCounts(3, 1) }
        override fun pendingByTarget(perTarget: Int): List<TargetQueue> { calls += "pending"; return emptyList() }
    }

    private val samples = CountingSamples()
    private val states = FakeStates()
    private val queue = CountingQueue()
    private val sender = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome = SendOutcome.Delivered }
    private fun round(silentGate: SilentGate? = null, expected: Set<String> = setOf("ci-agent-164")) =
        ThresholdRound(samples, states, ThresholdJudge(), AlertDispatcher(queue, sender), queue, expected, silentGate)

    // ── 판정과 보내기를 나눈다 ───────────────────────────

    @Test
    fun `판정 한 바퀴는 보내기를 부르지 않는다`() {
        val result = round().judgeOnce(now)
        assertTrue(result.judged)
        assertNull(result.dispatch)
        assertFalse(queue.calls.any { it in setOf("pending", "lock", "delivered", "retry", "rejected") }, "판정이 보내기를 불렀다: ${queue.calls}")
    }

    @Test
    fun `판정 예약이 부르는 함수는 판정만 한다`() {
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, thresholdRound = round())
        assertEquals(now, schedule.judgeThresholds(now)?.ranAt)
        assertEquals(listOf("resource:ci-agent-164:silent"), states.events.map { it.key })
        assertFalse(queue.calls.any { it in setOf("pending", "lock") }, "판정 예약이 보내기를 불렀다: ${queue.calls}")
    }

    @Test
    fun `보내기 한 바퀴는 판정하지 않고 보낸다 — 예약 함수도 그렇다`() {
        val oneRound = round()
        assertEquals(DispatchOutcome(true, 0, 0, 0), oneRound.dispatchOnce(now))
        assertEquals(0, samples.calls, "보내기가 표본을 읽었다(판정했다)")
        assertTrue("pending" in queue.calls)
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, thresholdRound = oneRound)
        assertEquals(DispatchOutcome(true, 0, 0, 0), schedule.dispatchAlerts(now))
        assertNull(ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore).dispatchAlerts(now))
    }

    @Test
    fun `보내기는 판정과 다른 예약이고 간격의 설정 키는 observe-alert-dispatch-interval-ms 다`() {
        val dispatch = ObserveSchedule::class.java.getMethod("dispatchAlerts").getAnnotation(Scheduled::class.java)
            ?: fail("dispatchAlerts() 에 @Scheduled 가 없다")
        assertEquals("\${observe.alert.dispatch-interval-ms:30000}", dispatch.fixedDelayString)
    }

    // ── 판정 입력 ─────────────────────────────────────

    @Test
    fun `판정 잠금을 얻지 못한 자리는 표본을 읽지 않는다`() {
        states.lockAvailable = false
        val result = round().judgeOnce(now)
        assertFalse(result.judged)
        assertEquals(0, samples.calls, "잠금 없이 표본을 읽었다")
        round().runOnce(now.plusSeconds(30))
        assertEquals(0, samples.calls, "runOnce 도 잠금 없이 표본을 읽었다")
    }

    // ── 끊김 판정의 시작 ─────────────────────────────

    @Test
    fun `끊김 판정 조건이 서지 않았으면 한 번도 안 보낸 기대 서버를 울리지 않는다`() {
        var caughtUp: Instant? = null
        val gated = round(SilentGate(now.minusSeconds(60), { caughtUp }))
        gated.judgeOnce(now)
        assertEquals(emptyList<String>(), states.events.map { it.key }, "뜬 지 1분인데 끊김을 울렸다")
        assertFalse(gated.silentJudging(now))
        caughtUp = now.minusSeconds(30)
        assertTrue(gated.silentJudging(now.plus(Duration.ofMinutes(2))))
        gated.judgeOnce(now.plus(Duration.ofMinutes(2)))
        assertEquals(listOf("resource:ci-agent-164:silent"), states.events.map { it.key })
        assertTrue(round().silentJudging(now), "조건이 없으면 늘 판정한다")
    }

    // ── 헬스는 가볍게 ─────────────────────────────────

    private val metricStream = FakeMetricStream()
    private val metricCollector = MetricCollector(metricStream, FakeMetricSink(), block = Duration.ZERO, clock = { now })
    private fun health(thresholds: ThresholdRound?, latest: (() -> List<ResourceSample>)? = null) = MetricHealth(
        metricCollector, { metricStream.unacknowledgedCount() }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null },
        startedAt = now, thresholds = thresholds, latestSamples = latest,
    )

    @Test
    fun `헬스는 보낼 표를 건드리지 않고 바퀴가 세어 둔 건수만 읽는다`() {
        metricCollector.runOnce()
        val oneRound = round()
        val before = health(oneRound).report(now)
        assertEquals(-1L, (before["undelivered_alerts"] as Number).toLong(), "아직 바퀴가 세지 않았는데 건수가 나왔다")
        assertEquals(emptyList<String>(), queue.calls, "헬스가 보낼 표(판정 · 보내기용 연결)를 건드렸다")
        oneRound.dispatchOnce(now)
        assertEquals(AlertCounts(3, 1), oneRound.lastAlertCounts)
        val callsAfterRound = queue.calls.size
        val after = health(oneRound).report(now)
        assertEquals(3L, (after["undelivered_alerts"] as Number).toLong())
        assertEquals(1L, (after["rejected_alerts"] as Number).toLong())
        assertEquals(callsAfterRound, queue.calls.size, "헬스가 보낼 표를 건드렸다: ${queue.calls}")
        val report = CollectorHealth(LogCollector(FakeLogStream(), FakeLogSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() },
            { 0L }, { 0L }, { null }, { null }, startedAt = now, metrics = health(oneRound)).report(now)
        assertEquals(true to "ok", report.healthy to report.body["status"], "로그의 판정이 바뀌었다")
    }

    @Test
    fun `헬스의 지표 칸에 끊김 판정을 하는 중인지가 나온다`() {
        metricCollector.runOnce()
        assertEquals(false, health(round(SilentGate(now.minusSeconds(10), { null })))
            .report(now)["silent_judging"])
        assertEquals(true, health(round()).report(now)["silent_judging"])
    }

    // ── 시계가 뒤처진 서버 ─────────────────────────────

    private fun latestOf(server: String, behind: Duration) = ResourceSample(StreamId(now.toEpochMilli(), 0), server, now.minus(behind), now, 30_000,
        HostValues(cpuPct = 1.0), emptyList(), emptyList(), emptyList())

    @Test
    fun `가장 최근 항목의 수신 − 측정이 1분을 넘는 서버만 시계가 뒤처진 서버다`() {
        val latest = listOf(latestOf("ci-agent-9", Duration.ofMinutes(1)), latestOf("ci-agent-164", Duration.ofMinutes(1).plusMillis(1)),
            latestOf("ci-agent-158", Duration.ofMinutes(7)), latestOf("ci-agent-1", Duration.ZERO))
        assertEquals(listOf("ci-agent-158", "ci-agent-164"), ClockBehind.servers(latest))
    }

    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }
    @AfterEach fun detach() { (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender) }

    @Test
    fun `헬스의 clock_behind 에 그 서버가 나오고 로그는 처음 1줄 · 풀리면 1줄`() {
        metricCollector.runOnce()
        var latest = listOf(latestOf("ci-agent-158", Duration.ofMinutes(5)), latestOf("ci-agent-164", Duration.ofSeconds(2)))
        val withClock = health(null) { latest }
        repeat(3) { assertEquals(listOf("ci-agent-158"), withClock.report(now)["clock_behind"]) }
        fun lines() = appender.list.count { it.formattedMessage.contains("ci-agent-158") }
        assertEquals(1, lines(), "뒤처진 동안 헬스마다 로그를 남겼다: ${appender.list.map { it.formattedMessage }}")
        latest = listOf(latestOf("ci-agent-158", Duration.ofSeconds(3)))
        repeat(2) { assertEquals(emptyList<String>(), withClock.report(now)["clock_behind"]) }
        assertEquals(2, lines(), "풀렸을 때 1줄이 아니다")
    }

    // ── 수집기의 따라잡음 ─────────────────────────────

    @Test
    fun `읽을 것이 없는 첫 바퀴가 따라잡은 시각이고 그 뒤로는 바뀌지 않는다`() {
        var clock = now
        val stream = FakeMetricStream()
        val collector = MetricCollector(stream, FakeMetricSink(), batchSize = 2, block = Duration.ZERO, clock = { clock })
        stream.add(*(1..5).map { MetricRecords.resource("${1000 + it}-0", now.minusSeconds(1)) }.toTypedArray())
        repeat(3) { clock = clock.plusSeconds(1); collector.runOnce() }        // 2 · 2 · 1 — 아직 읽을 것이 있었다
        assertNull(collector.caughtUpAt)
        clock = clock.plusSeconds(1)
        collector.runOnce()                                                     // 읽을 것이 없다
        assertEquals(clock, collector.caughtUpAt)
        val first = clock
        clock = clock.plusSeconds(10)
        collector.runOnce()
        assertEquals(first, collector.caughtUpAt)
    }

    private val unreachableLogStore = org.springframework.jdbc.core.JdbcTemplate(object : org.springframework.jdbc.datasource.AbstractDataSource() {
        override fun getConnection(): java.sql.Connection = throw java.sql.SQLException("Connection refused")
        override fun getConnection(username: String?, password: String?): java.sql.Connection = getConnection()
    }).let { xyz.junproject.observe.log.LogStore(it, it, it) }
}
