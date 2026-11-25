package xyz.junproject.observe.threshold

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricSampleSource
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant

/**
 * 보낼 표에 이 판이 모르는 규칙 글자의 줄이 있을 때(섞인 판 — 새 판이 쓴 줄) 보내기 바퀴가 그 수를 드러내는 길:
 * 보낼 표가 알려 준 규칙 글자마다의 수([AlertQueue.unknownRuleCounts])가 보내기 결과([DispatchOutcome.skippedUnknownRules])에 그대로 실리고,
 * 아는 줄의 보내기(차례 · 결과 수)는 그대로다 · 모르는 줄이 있으면 보내기 한 바퀴마다 `observe` 로거에 WARN 한 줄
 * (`unknown rule` 과 규칙 글자마다 `<글자>=<수>`, 합계 — 줄마다가 아니라 한 바퀴에 한 줄) · 없으면 그 WARN 이 없다 ·
 * 헬스의 지표 칸 `skipped_unknown_rule_alerts` 는 바퀴가 세어 둔 보낼 표의 모르는 줄 수([AlertCounts.unknownRule] — 세기 전 · 읽지 못하면 -1).
 * 보낼 표는 가짜(모르는 줄이 실제 표에서 어떻게 건너뛰어지는지는 엔진 테스트 `UnknownRuleAlertEngineTest`).
 */
class UnknownRuleDispatchTest {
    private val now = Instant.parse("2026-11-25T03:00:00Z")
    private var sequence = 0

    private fun alert(eventId: String, server: String, rule: ResourceRule = ResourceRule.MEMORY) =
        ResourceAlert(eventId, "resource:$server:${rule.wireName}", rule.kind, AlertSeverity.WARNING, "t", "d", server, rule, null,
            now.plusMillis((sequence++).toLong()))

    /** 아는 줄은 [alerts], 모르는 줄은 규칙 글자마다의 수 [unknown] 만 알려 주는 보낼 표 */
    private class Queue(alerts: List<ResourceAlert>, var unknown: Map<String, Long>, var lockAvailable: Boolean = true) : AlertQueue {
        val queued = alerts.mapIndexed { index, alert -> QueuedAlert(index + 1L, alert, 0, null) }.toMutableList()
        val lastAttempt = mutableMapOf<AlertTarget, Instant>()
        var counts: AlertCounts? = null
        private fun attempted(id: Long, at: Instant) { lastAttempt[targetOf(queued.first { it.id == id }.alert)] = at }
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun pendingByTarget(perTarget: Int) = targetQueues(queued, lastAttempt, perTarget)
        override fun markDelivered(id: Long, at: Instant) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { attempted(id, at) }
        override fun markRejected(id: Long, at: Instant, reason: String) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = if (lockAvailable) block() else null
        override fun alertCounts() = counts ?: AlertCounts(queued.size + unknown.values.sum(), 0, unknown.values.sum())
        override fun unknownRuleCounts(): Map<String, Long> = unknown
    }

    private class Sender(private val outcome: (ResourceAlert) -> SendOutcome = { SendOutcome.Delivered }) : AlertSender {
        val sent = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return outcome(alert) }
    }

    private val noSamples = object : MetricSampleSource {
        override fun samplesMeasuredBetween(from: Instant, to: Instant): List<ResourceSample> = emptyList()
        override fun lastReceivedOfEveryServer(): Map<String, Instant> = emptyMap()
    }
    private val noStates = object : RuleStateStore {
        override fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome = RoundOutcome(true, judge(emptyList()).events.size)
    }

    private fun round(queue: Queue, sender: Sender = Sender()) =
        ThresholdRound(noSamples, noStates, ThresholdJudge(), AlertDispatcher(queue, sender), queue, emptySet())

    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }
    @AfterEach fun detach() { (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender) }

    private fun unknownRuleWarnings() = appender.list.filter { it.level == Level.WARN && it.formattedMessage.contains("unknown rule") }

    // ── 보내기 결과 ───────────────────────────────────

    @Test
    fun `보낼 표가 알려 준 모르는 규칙의 줄 수가 보내기 결과에 그대로 실리고 아는 줄은 평소대로 간다`() {
        val unknown = mapOf("log_silent" to 4L, "log_silent_all" to 1L, "" to 1L)
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158"), alert("memory-2", "ci-agent-158"), alert("cpu-1", "ci-agent-164", ResourceRule.CPU)), unknown)
        val sender = Sender()
        assertEquals(DispatchOutcome(true, 3, 0, 0, unknown), AlertDispatcher(queue, sender).dispatch(now))
        assertEquals(listOf("memory-1", "cpu-1", "memory-2"), sender.sent, "아는 줄의 차례가 바뀌었다")
    }

    @Test
    fun `아는 줄이 하나도 없어도 모르는 줄의 수를 싣는다`() {
        val unknown = mapOf("log_silent" to 2L)
        assertEquals(DispatchOutcome(true, 0, 0, 0, unknown), AlertDispatcher(Queue(emptyList(), unknown), Sender()).dispatch(now))
    }

    @Test
    fun `보내기 잠금을 얻지 못한 자리는 아무것도 세지 않는다`() {
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 2L), lockAvailable = false)
        assertEquals(DispatchOutcome(false, 0, 0, 0), AlertDispatcher(queue, Sender()).dispatch(now))
        round(queue).dispatchOnce(now)
        assertEquals(emptyList<ILoggingEvent>(), unknownRuleWarnings(), "보내지 않은 자리가 WARN 을 남겼다")
    }

    // ── WARN 한 줄 ────────────────────────────────────

    @Test
    fun `모르는 줄이 있으면 보내기 한 바퀴마다 WARN 한 줄에 규칙 글자와 수 · 합계가 남는다`() {
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 4L, "log_silent_all" to 2L, "zzz-엉뚱한-글자" to 1L))
        val oneRound = round(queue)
        repeat(3) { oneRound.dispatchOnce(now.plusSeconds(30L * it)) }
        val warnings = unknownRuleWarnings()
        assertEquals(3, warnings.size, "한 바퀴에 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        for (warning in warnings) {
            for (part in listOf("log_silent=4", "log_silent_all=2", "zzz-엉뚱한-글자=1", "7")) {
                assertTrue(warning.formattedMessage.contains(part), "WARN 에 '$part' 가 없다: ${warning.formattedMessage}")
            }
        }
        assertEquals(DispatchOutcome(true, 0, 0, 0, queue.unknown), oneRound.dispatchOnce(now.plusSeconds(120)))
    }

    @Test
    fun `모르는 줄이 없으면 모르는 규칙의 WARN 을 남기지 않는다`() {
        val oneRound = round(Queue(listOf(alert("memory-1", "ci-agent-158")), emptyMap()))
        repeat(2) { oneRound.dispatchOnce(now.plusSeconds(30L * it)) }
        assertEquals(emptyList<ILoggingEvent>(), unknownRuleWarnings())
        assertFalse(appender.list.any { it.level == Level.WARN }, "모르는 줄이 없는데 WARN 이 남았다: ${appender.list.map { it.formattedMessage }}")
    }

    // ── 헬스 ─────────────────────────────────────────

    private val metricStream = FakeMetricStream()
    private val metricCollector = MetricCollector(metricStream, FakeMetricSink(), block = Duration.ZERO, clock = { now })
    private fun health(thresholds: ThresholdRound?) = MetricHealth(
        metricCollector, { metricStream.unacknowledgedCount() }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null },
        startedAt = now, thresholds = thresholds,
    ).report(now)

    @Test
    fun `헬스의 지표 칸 skipped_unknown_rule_alerts 는 바퀴가 세어 둔 모르는 줄의 수이고 세기 전에는 -1 이다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L))
        val oneRound = round(queue)
        assertEquals(-1L, (health(oneRound)["skipped_unknown_rule_alerts"] as? Number)?.toLong(), "세기 전인데 -1 이 아니거나 칸이 없다")
        queue.counts = AlertCounts(5, 1, 3)
        oneRound.dispatchOnce(now)
        val body = health(oneRound)
        assertEquals(3L, (body["skipped_unknown_rule_alerts"] as? Number)?.toLong(), "헬스에 모르는 줄의 수가 없다: $body")
        assertEquals(5L, (body["undelivered_alerts"] as Number).toLong())
        assertEquals("ok", body["status"], "모르는 줄이 지표 수집의 판정을 바꿨다")
        assertFalse(health(null).containsKey("skipped_unknown_rule_alerts"), "판정을 붙이지 않았는데 칸이 있다")
    }

    @Test
    fun `보낼 표의 건수를 읽지 못하면 헬스의 모르는 줄 수는 -1 이다`() {
        metricCollector.runOnce()
        val queue = object : AlertQueue by Queue(emptyList(), mapOf("log_silent" to 3L)) {
            override fun alertCounts(): AlertCounts = throw IllegalStateException("읽지 못함")
        }
        val oneRound = ThresholdRound(noSamples, noStates, ThresholdJudge(), AlertDispatcher(queue, Sender()), queue, emptySet())
        oneRound.dispatchOnce(now)
        assertEquals(-1L, (health(oneRound)["skipped_unknown_rule_alerts"] as? Number)?.toLong())
    }
}
