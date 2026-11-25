package xyz.junproject.observe.threshold

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.dao.QueryTimeoutException
import tools.jackson.databind.json.JsonMapper
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
 * 헬스의 지표 칸 `skipped_unknown_rule_alerts` 는 바퀴가 세어 둔 보낼 표의 모르는 줄 수([AlertCounts.unknownRule] — 세기 전 · 읽지 못하면 null).
 *
 * 세는 일은 보내기를 막지 않는다: 모르는 줄을 세는 조회는 그 바퀴의 아는 줄을 다 보낸 뒤에 한 번 불리고, 실패해도 그 바퀴의 보내기 결과는 그대로다 —
 * 그 바퀴의 건너뛴 수는 "세지 못함"([DispatchOutcome.skippedUnknownRules] 가 null)이고 `observe` 로거에 WARN 한 줄(예외의 종류 이름만 — 문구는 싣지 않는다).
 *
 * WARN 에 찍는 규칙 글자는 다듬는다(규칙 글자는 다른 판이 쓴 표의 값이다): 제어 문자([Character.isISOControl] — 줄바꿈 · 캐리지리턴 · 탭 · ESC · DEL · C1)는 `?` 로,
 * 글자마다 64자까지. 가짓수는 규칙 글자 순으로 앞의 10가지까지 찍고 나머지는 `+<가짓수> more` 로 줄인다(합계는 전부의 합). 보내기 결과에는 다듬지 않은 글자가 실린다.
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
        /** [alertCounts] 가 낼 예외 — null 이면 예외 없이 센다 */
        var alertCountsFailure: RuntimeException? = null
        /** 모르는 줄을 세는 조회가 하는 일 — 기본은 [unknown] 을 돌려준다 */
        var counting: () -> Map<String, Long> = { unknown }
        /** 표를 바꾼 일과 세는 조회의 차례 — `delivered:<알림>` · `retry:<알림>` · `rejected:<알림>` · `count` */
        val calls = mutableListOf<String>()
        private fun attempted(kind: String, id: Long, at: Instant) {
            val alert = queued.first { it.id == id }.alert
            calls += "$kind:${alert.eventId}"
            lastAttempt[targetOf(alert)] = at
        }
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun pendingByTarget(perTarget: Int) = targetQueues(queued, lastAttempt, perTarget)
        override fun markDelivered(id: Long, at: Instant) { attempted("delivered", id, at); queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { attempted("retry", id, at) }
        override fun markRejected(id: Long, at: Instant, reason: String) { attempted("rejected", id, at); queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = if (lockAvailable) block() else null
        override fun alertCounts(): AlertCounts {
            alertCountsFailure?.let { throw it }
            return counts ?: AlertCounts(queued.size + unknown.values.sum(), 0, unknown.values.sum())
        }
        override fun unknownRuleCounts(): Map<String, Long> { calls += "count"; return counting() }
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
        assertEquals(emptyList<String>(), queue.calls, "보내기 잠금을 얻지 못한 자리가 모르는 줄을 셌거나 표를 바꿨다")
    }

    // ── 세는 일이 보내기를 막지 않는다 ─────────────────

    /** 아는 줄 넷 — 한 대상(ci-agent-158 메모리)에 둘, 나머지는 대상이 다르다. 보내는 쪽은 cpu-1 을 거절, disk-1 은 다시 시도 */
    private fun knownQueue(unknown: Map<String, Long>) = Queue(listOf(
        alert("memory-1", "ci-agent-158"), alert("memory-2", "ci-agent-158"),
        alert("cpu-1", "ci-agent-164", ResourceRule.CPU), alert("disk-1", "ci-agent-9", ResourceRule.DISK_WARNING),
    ), unknown)

    private fun mixedSender() = Sender { when (it.eventId) { "cpu-1" -> SendOutcome.Rejected("http 422"); "disk-1" -> SendOutcome.Retry("http 503"); else -> SendOutcome.Delivered } }

    private val expectedSent = listOf("memory-1", "cpu-1", "disk-1", "memory-2")
    private val expectedMarks = listOf("delivered:memory-1", "rejected:cpu-1", "retry:disk-1", "delivered:memory-2")

    /** 세는 조회가 내는 예외 — 문구에는 로그에 옮기면 안 되는 글을 둔다 */
    private fun countFailure() = QueryTimeoutException("count canceled on jdbc:postgresql://observe-db:5432/observe password=do-not-log")

    @Test
    fun `모르는 줄을 세는 조회가 실패해도 그 바퀴의 아는 줄은 전부 보내지고 결과 수는 그대로이며 건너뛴 수는 세지 못함(null)이다`() {
        val baselineQueue = knownQueue(mapOf("log_silent" to 2L))
        val baselineSender = mixedSender()
        assertEquals(DispatchOutcome(true, 2, 1, 1, mapOf("log_silent" to 2L)), AlertDispatcher(baselineQueue, baselineSender).dispatch(now))
        assertEquals(expectedSent, baselineSender.sent)

        val queue = knownQueue(mapOf("log_silent" to 2L)).apply { counting = { throw countFailure() } }
        val sender = mixedSender()
        val outcome = assertDoesNotThrow<DispatchOutcome>({ AlertDispatcher(queue, sender).dispatch(now) }, "세는 조회의 실패가 보내기 바퀴를 실패로 만들었다")
        assertEquals(expectedSent, sender.sent, "세는 조회가 실패하자 아는 줄을 다 보내지 않았거나 차례가 바뀌었다")
        assertEquals(expectedMarks, queue.calls.filter { it != "count" }, "보낸 결과의 표시가 세는 조회가 성공할 때와 다르다")
        assertTrue(outcome.lockAcquired)
        assertEquals(listOf(2, 1, 1), listOf(outcome.delivered, outcome.retrying, outcome.rejected), "보내기 결과 수가 바뀌었다: $outcome")
        assertNull(outcome.skippedUnknownRules, "세지 못한 바퀴의 건너뛴 수가 '세지 못함'(null)이 아니다: $outcome")
        assertEquals(listOf("disk-1"), queue.queued.map { it.alert.eventId }, "보낸 줄 · 거절된 줄이 남았거나 다시 시도할 줄이 사라졌다")
    }

    @Test
    fun `모르는 줄을 세지 못한 바퀴는 예외의 종류 이름만 담은 WARN 한 줄을 남기고 보내기 바퀴는 실패로 남지 않는다`() {
        val queue = knownQueue(mapOf("log_silent" to 2L)).apply { counting = { throw countFailure() } }
        val sender = mixedSender()
        val oneRound = round(queue, sender)
        val outcome = oneRound.dispatchOnce(now)
        assertNotNull(outcome, "세는 조회의 실패가 보내기 바퀴를 실패로 만들었다")
        assertNull(oneRound.lastDispatchFailure, "세는 조회의 실패가 보내기 실패로 남았다")
        assertEquals(listOf(2, 1, 1), listOf(outcome!!.delivered, outcome.retrying, outcome.rejected))
        assertNull(outcome.skippedUnknownRules)
        assertEquals(expectedSent, sender.sent)

        val warnings = appender.list.filter { it.level == Level.WARN }
        assertEquals(1, warnings.size, "세지 못한 바퀴의 WARN 이 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        val warning = warnings.single().formattedMessage
        assertTrue(warning.contains("unknown rule"), "WARN 이 모르는 규칙의 줄을 세지 못한 것임을 말하지 않는다: $warning")
        assertTrue(warning.contains("QueryTimeoutException"), "WARN 에 예외의 종류 이름이 없다: $warning")
        for (secret in listOf("jdbc:postgresql", "observe-db", "do-not-log", "count canceled")) {
            assertFalse(warning.contains(secret), "WARN 에 예외의 문구가 실렸다('$secret'): $warning")
        }
        assertNull(warnings.single().throwableProxy, "WARN 에 예외(문구 · 스택)가 붙었다")
        assertFalse(appender.list.any { it.level == Level.ERROR }, "세는 조회의 실패가 ERROR 로 남았다: ${appender.list.map { it.formattedMessage }}")
    }

    @Test
    fun `모르는 줄을 세는 조회는 그 바퀴의 아는 줄을 다 보내고 표시한 뒤에 한 번 불린다`() {
        val queue = knownQueue(mapOf("log_silent" to 2L))
        AlertDispatcher(queue, mixedSender()).dispatch(now)
        assertEquals(expectedMarks + "count", queue.calls, "세는 조회가 보내기보다 먼저 불렸거나 한 번이 아니다")

        val empty = Queue(emptyList(), mapOf("log_silent" to 2L))
        assertEquals(DispatchOutcome(true, 0, 0, 0, mapOf("log_silent" to 2L)), AlertDispatcher(empty, Sender()).dispatch(now))
        assertEquals(listOf("count"), empty.calls, "보낼 아는 줄이 없을 때 세는 조회가 한 번이 아니다")
    }

    @Test
    fun `모르는 줄을 세는 조회가 바퀴의 상한보다 오래 걸려도 아는 줄은 그 전에 이미 다 나갔다`() {
        var clock = now
        val queue = knownQueue(mapOf("log_silent" to 2L)).apply { counting = { clock = clock.plusSeconds(60); unknown } }
        val sender = mixedSender()
        val outcome = AlertDispatcher(queue, sender, clock = { clock }).dispatch(now)
        assertEquals(expectedSent, sender.sent, "느린 세는 조회가 바퀴의 상한을 먹어 아는 줄이 나가지 못했다")
        assertEquals(DispatchOutcome(true, 2, 1, 1, mapOf("log_silent" to 2L)), outcome)
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

    // ── WARN 에 찍는 규칙 글자 ──────────────────────────

    private fun singleUnknownRuleWarning(unknown: Map<String, Long>): String {
        appender.list.clear()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), unknown)
        val outcome = round(queue).dispatchOnce(now)
        assertEquals(unknown, outcome?.skippedUnknownRules, "보내기 결과에 다듬지 않은 규칙 글자와 수가 실리지 않았다")
        val warnings = unknownRuleWarnings()
        assertEquals(1, warnings.size, "한 바퀴에 WARN 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        return warnings.single().formattedMessage
    }

    @Test
    fun `WARN 에 찍는 규칙 글자는 제어 문자가 물음표로 바뀌고 글자마다 64자를 넘지 않으며 보통의 글자는 그대로다`() {
        val forged = "log_silent\nobserve:observe:FAKE" + "x".repeat(5_000)
        val controls = "tab\there\r\u0000\u001b[31m\u007f\u0085end"
        val warning = singleUnknownRuleWarning(mapOf(forged to 2L, "log_silent" to 3L, controls to 1L))

        val controlCharacters = warning.filter { Character.isISOControl(it) }
        assertTrue(controlCharacters.isEmpty(), "WARN 에 제어 문자가 남았다(${controlCharacters.map { it.code }}): ${warning.take(200)}")
        assertTrue(warning.contains("log_silent=3"), "보통의 글자가 그대로 찍히지 않았다: ${warning.take(300)}")
        assertTrue(warning.contains("tab?here???[31m??end=1"), "제어 문자가 하나씩 '?' 로 바뀌지 않았다: ${warning.take(300)}")

        val forgedStart = warning.indexOf("log_silent?observe:observe:FAKE")
        assertTrue(forgedStart >= 0, "줄바꿈이 '?' 로 바뀐 규칙 글자가 없다: ${warning.take(300)}")
        val forgedPrinted = warning.substring(forgedStart, warning.indexOf('=', forgedStart))
        assertTrue(forgedPrinted.length <= 64, "규칙 글자가 64자를 넘게 찍혔다(${forgedPrinted.length}자)")
        assertTrue(forgedPrinted.startsWith("log_silent?observe:observe:FAKExxxx"), "규칙 글자의 앞부분이 그대로 찍히지 않았다: $forgedPrinted")
        assertTrue(warning.contains("$forgedPrinted=2"), "다듬은 규칙 글자 뒤에 그 수가 없다: ${warning.take(300)}")
        assertFalse(warning.contains("x".repeat(65)), "규칙 글자가 잘리지 않았다")
        assertTrue(warning.contains("total 6"), "합계가 없다: ${warning.take(300)}")
    }

    @Test
    fun `규칙의 가짓수가 10 을 넘으면 WARN 한 줄은 규칙 글자 순으로 앞의 10가지만 찍고 나머지는 가짓수로 줄인다`() {
        fun rules(count: Int) = (0 until count).associate { "r%03d-".format(it) + "y".repeat(5_000) to 1L }
        fun listed(warning: String) = Regex("r\\d{3}-").findAll(warning).map { it.value }.toList()

        val ten = singleUnknownRuleWarning(rules(10))
        assertEquals((0 until 10).map { "r%03d-".format(it) }, listed(ten), "10가지일 때 전부 찍히지 않았다")
        assertFalse(ten.contains("more"), "10가지인데 줄였다: ${ten.take(300)}")
        assertTrue(ten.contains("total 10"))

        val eleven = singleUnknownRuleWarning(rules(11))
        assertEquals((0 until 10).map { "r%03d-".format(it) }, listed(eleven), "11가지일 때 앞의 10가지만 찍히지 않았다")
        assertTrue(eleven.contains("+1 more"), "나머지 가짓수가 없다: ${eleven.takeLast(200)}")
        assertTrue(eleven.contains("total 11"), "합계가 찍힌 가짓수만의 합이다: ${eleven.takeLast(200)}")

        val many = singleUnknownRuleWarning(rules(500))
        assertEquals((0 until 10).map { "r%03d-".format(it) }, listed(many))
        assertTrue(many.contains("+490 more"), "나머지 가짓수가 없다: ${many.takeLast(200)}")
        assertTrue(many.contains("total 500"))
        assertTrue(many.length <= 1_500, "500가지 · 글자마다 5,000자일 때 WARN 한 줄이 ${many.length}자다(상한 1,500)")
    }

    // ── 헬스 ─────────────────────────────────────────

    private val metricStream = FakeMetricStream()
    private val metricCollector = MetricCollector(metricStream, FakeMetricSink(), block = Duration.ZERO, clock = { now })
    private fun health(thresholds: ThresholdRound?) = MetricHealth(
        metricCollector, { metricStream.unacknowledgedCount() }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null },
        startedAt = now, thresholds = thresholds,
    ).report(now)

    private fun skippedInHealth(body: Map<String, Any?>): Any? {
        assertTrue(body.containsKey("skipped_unknown_rule_alerts"), "헬스에 skipped_unknown_rule_alerts 칸이 없다: $body")
        return body["skipped_unknown_rule_alerts"]
    }

    @Test
    fun `헬스의 지표 칸 skipped_unknown_rule_alerts 는 바퀴가 세어 둔 모르는 줄의 수이고 세기 전에는 null 이다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L))
        val oneRound = round(queue)
        assertNull(skippedInHealth(health(oneRound)), "세기 전인데 수가 나왔다")
        queue.counts = AlertCounts(5, 1, 3)
        oneRound.dispatchOnce(now)
        val body = health(oneRound)
        assertEquals(3L, (skippedInHealth(body) as? Number)?.toLong(), "헬스에 모르는 줄의 수가 없다: $body")
        assertEquals(5L, (body["undelivered_alerts"] as Number).toLong())
        assertEquals("ok", body["status"], "모르는 줄이 지표 수집의 판정을 바꿨다")
        assertFalse(health(null).containsKey("skipped_unknown_rule_alerts"), "판정을 붙이지 않았는데 칸이 있다")
    }

    @Test
    fun `보낼 표의 건수를 세지 못한 바퀴 뒤 헬스의 모르는 줄 수는 JSON 의 null 이고 다음 바퀴에 세면 다시 수가 나온다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L))
        val oneRound = round(queue)
        queue.counts = AlertCounts(4, 0, 3)
        oneRound.dispatchOnce(now)
        assertEquals(3L, (skippedInHealth(health(oneRound)) as? Number)?.toLong())

        queue.alertCountsFailure = QueryTimeoutException("count canceled")
        oneRound.dispatchOnce(now.plusSeconds(30))
        val failed = health(oneRound)
        assertNull(skippedInHealth(failed), "세지 못했는데 수가 나왔다(0 · -1 · 앞 바퀴의 수로 꾸몄다)")
        assertEquals(-1L, (failed["undelivered_alerts"] as Number).toLong(), "못 보낸 수의 '읽지 못함'(-1)이 바뀌었다")
        val json = JsonMapper.builder().build().writeValueAsString(failed)
        assertTrue(json.contains("\"skipped_unknown_rule_alerts\":null"), "헬스 본문의 JSON 에서 세지 못한 수가 null 이 아니다: $json")

        queue.alertCountsFailure = null
        queue.counts = AlertCounts(6, 0, 2)
        oneRound.dispatchOnce(now.plusSeconds(60))
        assertEquals(2L, (skippedInHealth(health(oneRound)) as? Number)?.toLong(), "다음 바퀴에 셌는데 수가 돌아오지 않았다")
    }

    @Test
    fun `보내기 잠금을 얻지 못한 바퀴도 보낼 표의 건수는 세어 헬스의 모르는 줄 수가 나온다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L), lockAvailable = false)
        val oneRound = round(queue)
        queue.alertCountsFailure = QueryTimeoutException("count canceled")
        oneRound.dispatchOnce(now)
        assertNull(skippedInHealth(health(oneRound)))

        queue.alertCountsFailure = null
        queue.counts = AlertCounts(5, 0, 3)
        assertEquals(DispatchOutcome(false, 0, 0, 0), oneRound.dispatchOnce(now.plusSeconds(30)))
        assertEquals(3L, (skippedInHealth(health(oneRound)) as? Number)?.toLong(), "잠금을 얻지 못한 바퀴 뒤 보낼 표의 모르는 줄 수가 헬스에 없다")
        assertEquals(emptyList<String>(), queue.calls, "잠금을 얻지 못한 자리가 모르는 줄을 셌다")
    }
}
