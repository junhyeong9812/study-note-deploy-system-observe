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
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.time.Duration
import java.time.Instant

/**
 * 보낼 표에 이 판이 모르는 규칙 글자의 줄이 있을 때(섞인 판 — 새 판이 쓴 줄) 그 수를 드러내는 길 — **세는 길은 하나다**:
 * 보내는 쪽([AlertDispatcher])은 모르는 줄을 세지 않는다(보낼 표의 세는 조회를 부르지 않는다). 바퀴가 끝날 때 보내기 잠금 밖에서
 * 보낼 표의 건수([AlertQueue.alertCounts])를 한 번 세고, 그 결과의 규칙 글자별 맵([AlertCounts.unknownRules])을 WARN 과 헬스가 함께 쓴다:
 * - 세었고 모르는 줄이 있으면 `observe` 로거에 WARN 한 줄(`unknown rule` 과 규칙 글자마다 `<글자>=<수>`, `total <합>`) · 헬스의
 *   `skipped_unknown_rule_alerts` = 그 맵의 합. 모르는 줄이 없으면 WARN 이 없고 헬스는 0.
 * - 세지 못하면 WARN 한 줄(`unknown rule` 과 예외의 종류 이름만 — 문구 · 예외 객체는 싣지 않는다) · 헬스는 null(JSON 의 null). 그 바퀴의 보내기 결과는 그대로다.
 * - 보내기 잠금을 얻지 못한 바퀴도 센다(헬스는 지금 남은 수). 판정 바퀴의 세기는 헬스만 바꾸고 이 WARN 을 남기지 않는다.
 *
 * WARN 에 찍는 규칙 글자는 다듬는다(규칙 글자는 다른 판이 쓴 표의 값이다): 제어 문자([Character.isISOControl])와
 * 줄 · 문단 구분 문자(U+2028 · U+2029) · 서식 문자(Cf — U+200B · U+202E · U+FEFF 등)는 `?` 로, 글자마다 64 코드포인트까지(서로게이트 쌍을 쪼개지 않는다).
 * 가짓수는 규칙 글자 순으로 앞의 10가지까지 찍고 나머지는 `(+<가짓수> more)` 로 줄인다(합계는 전부의 합).
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
        /** [alertCounts] 가 낼 예외 — null 이면 예외 없이 센다 */
        var alertCountsFailure: RuntimeException? = null
        /** 건수 세기가 돌려줄 값 — 기본은 남은 아는 줄 + 모르는 줄, 모르는 줄은 [unknown] */
        var counting: () -> AlertCounts = { AlertCounts(queued.size + unknown.values.sum(), 0, unknown) }
        /** 잠금 · 표시 · 건수 세기의 차례 — `lock:acquired` · `lock:released` · `lock:missed` · `delivered:<알림>` · `retry:<알림>` · `rejected:<알림>` · `alertCounts` */
        val calls = mutableListOf<String>()
        /** [watched] 로 감싼 표에 불린 메서드 이름 전부(보낼 표의 면에 있는 어떤 메서드든) */
        val methods = mutableListOf<String>()
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
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? {
            if (!lockAvailable) { calls += "lock:missed"; return null }
            calls += "lock:acquired"
            try { return block() } finally { calls += "lock:released" }
        }
        override fun alertCounts(): AlertCounts {
            calls += "alertCounts"
            alertCountsFailure?.let { throw it }
            return counting()
        }
    }

    /** 보낼 표의 면에 불린 메서드를 모두 [Queue.methods] 에 남기는 겉 — 가짜가 덮어쓰지 않은(기본 구현의) 메서드가 불려도 남는다 */
    private fun watched(queue: Queue): AlertQueue =
        Proxy.newProxyInstance(AlertQueue::class.java.classLoader, arrayOf(AlertQueue::class.java)) { _, method, arguments ->
            queue.methods += method.name
            try { method.invoke(queue, *(arguments ?: emptyArray())) } catch (error: InvocationTargetException) { throw error.targetException }
        } as AlertQueue

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

    /** 보내는 쪽과 바퀴가 같은 감싼 표를 쓴다 */
    private fun round(queue: Queue, sender: Sender = Sender()): ThresholdRound {
        val table = watched(queue)
        return ThresholdRound(noSamples, noStates, ThresholdJudge(), AlertDispatcher(table, sender), table, emptySet())
    }

    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }
    @AfterEach fun detach() { (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender) }

    private fun unknownRuleWarnings() = appender.list.filter { it.level == Level.WARN && it.formattedMessage.contains("unknown rule") }

    private fun fields(outcome: DispatchOutcome?) = outcome?.let { listOf(it.lockAcquired, it.delivered, it.retrying, it.rejected) }

    /** 보내는 쪽이 부를 수 있는 보낼 표의 면 — 세는 조회(건수 세기 · 모르는 줄 세기)는 여기에 없다 */
    private val dispatchMethods = setOf("whileHoldingDispatchLock", "pendingByTarget", "pendingAlerts", "markDelivered", "markRetry", "markRejected")

    // ── 보내는 쪽은 세지 않는다 ─────────────────────────

    @Test
    fun `보내는 쪽은 모르는 줄을 세지 않고 아는 줄은 평소대로 간다`() {
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158"), alert("memory-2", "ci-agent-158"), alert("cpu-1", "ci-agent-164", ResourceRule.CPU)),
            mapOf("log_silent" to 4L, "log_silent_all" to 1L, "" to 1L))
        val sender = Sender()
        val outcome = AlertDispatcher(watched(queue), sender).dispatch(now)
        assertEquals(listOf(true, 3, 0, 0), fields(outcome))
        assertEquals(listOf("memory-1", "cpu-1", "memory-2"), sender.sent, "아는 줄의 차례가 바뀌었다")
        assertEquals(emptyList<String>(), queue.methods.filterNot { it in dispatchMethods }, "보내는 쪽이 보낼 표를 셌다")
    }

    @Test
    fun `보내기 잠금을 얻지 못한 보내는 쪽은 아무것도 세지도 보내지도 않는다`() {
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 2L), lockAvailable = false)
        val sender = Sender()
        assertEquals(listOf(false, 0, 0, 0), fields(AlertDispatcher(watched(queue), sender).dispatch(now)))
        assertEquals(listOf("whileHoldingDispatchLock"), queue.methods, "잠금을 얻지 못한 보내는 쪽이 보낼 표를 더 불렀다")
        assertEquals(emptyList<String>(), sender.sent)
    }

    // ── 바퀴 끝의 건수 세기 — 잠금 밖에서 한 번 ─────────────

    /** 아는 줄 넷 — 한 대상(ci-agent-158 메모리)에 둘, 나머지는 대상이 다르다. 보내는 쪽은 cpu-1 을 거절, disk-1 은 다시 시도 */
    private fun knownQueue(unknown: Map<String, Long>) = Queue(listOf(
        alert("memory-1", "ci-agent-158"), alert("memory-2", "ci-agent-158"),
        alert("cpu-1", "ci-agent-164", ResourceRule.CPU), alert("disk-1", "ci-agent-9", ResourceRule.DISK_WARNING),
    ), unknown)

    private fun mixedSender() = Sender { when (it.eventId) { "cpu-1" -> SendOutcome.Rejected("http 422"); "disk-1" -> SendOutcome.Retry("http 503"); else -> SendOutcome.Delivered } }

    private val expectedSent = listOf("memory-1", "cpu-1", "disk-1", "memory-2")
    private val expectedMarks = listOf("delivered:memory-1", "rejected:cpu-1", "retry:disk-1", "delivered:memory-2")

    @Test
    fun `보내기 바퀴의 건수 세기는 보내기 잠금을 놓은 뒤에 한 번 불리고 모르는 줄을 따로 세는 조회는 없다`() {
        val queue = knownQueue(mapOf("log_silent" to 2L))
        round(queue, mixedSender()).dispatchOnce(now)
        assertEquals(listOf("lock:acquired") + expectedMarks + listOf("lock:released", "alertCounts"), queue.calls,
            "건수 세기가 보내기 잠금 안에서 불렸거나 한 번이 아니다")
        assertEquals(1, queue.methods.count { it == "alertCounts" }, "한 바퀴에 건수 세기가 한 번이 아니다: ${queue.methods}")
        assertEquals(emptyList<String>(), queue.methods.filterNot { it in dispatchMethods || it == "alertCounts" }, "건수 세기 말고 다른 세는 조회가 불렸다")
    }

    @Test
    fun `보내기 잠금을 얻지 못한 바퀴도 건수를 한 번 세어 WARN 과 헬스에 지금 남은 모르는 줄의 수가 나온다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L), lockAvailable = false)
        val oneRound = round(queue)
        assertEquals(listOf(false, 0, 0, 0), fields(oneRound.dispatchOnce(now)))
        assertEquals(listOf("lock:missed", "alertCounts"), queue.calls)
        assertEquals(3L, (skippedInHealth(health(oneRound)) as? Number)?.toLong(), "잠금을 얻지 못한 바퀴 뒤 헬스에 모르는 줄의 수가 없다")
        val warnings = unknownRuleWarnings()
        assertEquals(1, warnings.size, "잠금을 얻지 못한 바퀴의 WARN 이 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        assertTrue(warnings.single().formattedMessage.contains("log_silent=3"), warnings.single().formattedMessage)
    }

    @Test
    fun `WARN 과 헬스는 같은 한 번의 건수 세기를 쓴다`() {
        metricCollector.runOnce()
        val queue = knownQueue(emptyMap())
        val answers = ArrayDeque(listOf(mapOf("log_silent" to 3L), mapOf("log_silent" to 7L), mapOf("log_silent" to 11L)))
        queue.counting = { AlertCounts(10, 0, answers.removeFirst()) }
        val oneRound = round(queue, mixedSender())
        oneRound.dispatchOnce(now)
        val warning = unknownRuleWarnings().single().formattedMessage
        assertTrue(warning.contains("log_silent=3") && warning.contains("total 3"), "WARN 이 그 바퀴의 첫 세기를 쓰지 않았다: $warning")
        assertEquals(3L, (skippedInHealth(health(oneRound)) as? Number)?.toLong(), "헬스와 WARN 이 다른 수를 말한다")
        assertEquals(1, queue.methods.count { it == "alertCounts" }, "한 바퀴에 건수 세기가 한 번이 아니다: ${queue.methods}")
    }

    /** 세는 조회가 내는 예외 — 문구에는 로그에 옮기면 안 되는 글을 둔다 */
    private fun countFailure() = QueryTimeoutException("count canceled on jdbc:postgresql://observe-db:5432/observe password=do-not-log")

    @Test
    fun `건수를 세지 못한 바퀴도 아는 줄은 전부 보내지고 결과 수는 그대로이며 보내기 바퀴는 실패로 남지 않는다`() {
        val queue = knownQueue(mapOf("log_silent" to 2L)).apply { alertCountsFailure = countFailure() }
        val sender = mixedSender()
        val oneRound = round(queue, sender)
        val outcome = oneRound.dispatchOnce(now)
        assertEquals(listOf(true, 2, 1, 1), fields(outcome), "세기의 실패가 보내기 결과를 바꿨다")
        assertNull(oneRound.lastDispatchFailure, "세기의 실패가 보내기 실패로 남았다")
        assertEquals(expectedSent, sender.sent, "세기가 실패하자 아는 줄을 다 보내지 않았거나 차례가 바뀌었다")
        assertEquals(listOf("disk-1"), queue.queued.map { it.alert.eventId }, "보낸 줄 · 거절된 줄이 남았거나 다시 시도할 줄이 사라졌다")
    }

    @Test
    fun `건수를 세지 못한 바퀴는 예외의 종류 이름만 담은 WARN 한 줄을 남기고 헬스의 모르는 줄 수는 JSON 의 null 이다`() {
        metricCollector.runOnce()
        val queue = knownQueue(mapOf("log_silent" to 2L)).apply { alertCountsFailure = countFailure() }
        val oneRound = round(queue, mixedSender())
        oneRound.dispatchOnce(now)

        val warnings = appender.list.filter { it.level == Level.WARN }
        assertEquals(1, warnings.size, "세지 못한 바퀴의 WARN 이 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        val warning = warnings.single().formattedMessage
        assertTrue(warning.contains("unknown rule"), "WARN 이 모르는 규칙의 줄을 세지 못한 것임을 말하지 않는다: $warning")
        assertTrue(warning.contains("QueryTimeoutException"), "WARN 에 예외의 종류 이름이 없다: $warning")
        for (secret in listOf("jdbc:postgresql", "observe-db", "do-not-log", "count canceled")) {
            assertFalse(warning.contains(secret), "WARN 에 예외의 문구가 실렸다('$secret'): $warning")
        }
        assertNull(warnings.single().throwableProxy, "WARN 에 예외(문구 · 스택)가 붙었다")
        assertFalse(appender.list.any { it.level == Level.ERROR }, "세기의 실패가 ERROR 로 남았다: ${appender.list.map { it.formattedMessage }}")

        val body = health(oneRound)
        assertNull(skippedInHealth(body), "세지 못했는데 수가 나왔다(0 · -1 · 앞 바퀴의 수로 꾸몄다)")
        assertEquals(-1L, (body["undelivered_alerts"] as Number).toLong(), "못 보낸 수의 '읽지 못함'(-1)이 바뀌었다")
        val json = JsonMapper.builder().build().writeValueAsString(body)
        assertTrue(json.contains("\"skipped_unknown_rule_alerts\":null"), "헬스 본문의 JSON 에서 세지 못한 수가 null 이 아니다: $json")
    }

    @Test
    fun `건수를 세지 못한 다음 바퀴에 세면 헬스에 다시 수가 나온다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), mapOf("log_silent" to 3L))
        val oneRound = round(queue)
        oneRound.dispatchOnce(now)
        assertEquals(3L, (skippedInHealth(health(oneRound)) as? Number)?.toLong())

        queue.alertCountsFailure = countFailure()
        oneRound.dispatchOnce(now.plusSeconds(30))
        assertNull(skippedInHealth(health(oneRound)))

        queue.alertCountsFailure = null
        queue.unknown = mapOf("log_silent" to 1L, "log_silent_all" to 1L)
        oneRound.dispatchOnce(now.plusSeconds(60))
        assertEquals(2L, (skippedInHealth(health(oneRound)) as? Number)?.toLong(), "다음 바퀴에 셌는데 수가 돌아오지 않았다")
    }

    @Test
    fun `판정 바퀴의 건수 세기는 헬스의 수만 바꾸고 모르는 규칙의 WARN 을 남기지 않는다`() {
        metricCollector.runOnce()
        val queue = Queue(emptyList(), mapOf("log_silent" to 5L))
        val oneRound = round(queue)
        oneRound.judgeOnce(now)
        assertEquals(1, queue.methods.count { it == "alertCounts" })
        assertEquals(5L, (skippedInHealth(health(oneRound)) as? Number)?.toLong())
        assertEquals(emptyList<ILoggingEvent>(), unknownRuleWarnings(), "판정 바퀴가 모르는 규칙의 WARN 을 남겼다")
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
            for (part in listOf("log_silent=4", "log_silent_all=2", "zzz-엉뚱한-글자=1", "total 7")) {
                assertTrue(warning.formattedMessage.contains(part), "WARN 에 '$part' 가 없다: ${warning.formattedMessage}")
            }
        }
    }

    @Test
    fun `모르는 줄이 없으면 모르는 규칙의 WARN 을 남기지 않고 헬스의 수는 0 이다`() {
        metricCollector.runOnce()
        val oneRound = round(Queue(listOf(alert("memory-1", "ci-agent-158")), emptyMap()))
        repeat(2) { oneRound.dispatchOnce(now.plusSeconds(30L * it)) }
        assertEquals(emptyList<ILoggingEvent>(), unknownRuleWarnings())
        assertFalse(appender.list.any { it.level == Level.WARN }, "모르는 줄이 없는데 WARN 이 남았다: ${appender.list.map { it.formattedMessage }}")
        assertEquals(0L, (skippedInHealth(health(oneRound)) as? Number)?.toLong())
    }

    // ── WARN 에 찍는 규칙 글자 ──────────────────────────

    private fun singleUnknownRuleWarning(unknown: Map<String, Long>): String {
        appender.list.clear()
        round(Queue(listOf(alert("memory-1", "ci-agent-158")), unknown)).dispatchOnce(now)
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
    fun `규칙 글자는 코드포인트 경계에서 64까지 자르고 줄 · 문단 구분 문자와 서식 문자도 물음표로 바뀐다`() {
        val emojiAtCut = "a".repeat(63) + "😀" + "tail"          // 63자 + 이모지(서로게이트 쌍) — 글자 수로 64에서 자르면 쌍이 쪼개진다
        val separators = "x y z​w‮v﻿u"
        val warning = singleUnknownRuleWarning(mapOf(emojiAtCut to 1L, separators to 2L))

        warning.forEachIndexed { index, character ->
            if (Character.isHighSurrogate(character)) assertTrue(index + 1 < warning.length && Character.isLowSurrogate(warning[index + 1]), "짝 없는 상위 서로게이트가 찍혔다(위치 $index)")
            if (Character.isLowSurrogate(character)) assertTrue(index > 0 && Character.isHighSurrogate(warning[index - 1]), "짝 없는 하위 서로게이트가 찍혔다(위치 $index)")
        }
        val emojiStart = warning.indexOf("a".repeat(63))
        assertTrue(emojiStart >= 0, "규칙 글자의 앞 63자가 그대로 찍히지 않았다: ${warning.take(300)}")
        val emojiPrinted = warning.substring(emojiStart, warning.indexOf('=', emojiStart))
        assertTrue(emojiPrinted.codePointCount(0, emojiPrinted.length) <= 64, "규칙 글자가 64 코드포인트를 넘게 찍혔다: $emojiPrinted")
        assertFalse(emojiPrinted.contains("tail"), "규칙 글자가 잘리지 않았다: $emojiPrinted")

        for (hidden in listOf(' ', ' ', '​', '‮', '﻿')) {
            assertFalse(warning.contains(hidden), "WARN 에 U+%04X 가 남았다".format(hidden.code))
        }
        assertTrue(warning.contains("x?y?z?w?v?u=2"), "구분 문자 · 서식 문자가 하나씩 '?' 로 바뀌지 않았다: ${warning.take(300)}")
        assertTrue(warning.contains("total 3"))
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
        assertTrue(eleven.contains("(+1 more)"), "나머지 가짓수가 없다: ${eleven.takeLast(200)}")
        assertTrue(eleven.contains("total 11"), "합계가 찍힌 가짓수만의 합이다: ${eleven.takeLast(200)}")

        val many = singleUnknownRuleWarning(rules(500))
        assertEquals((0 until 10).map { "r%03d-".format(it) }, listed(many))
        assertTrue(many.contains("(+490 more)"), "나머지 가짓수가 없다: ${many.takeLast(200)}")
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
    fun `헬스의 지표 칸 skipped_unknown_rule_alerts 는 바퀴가 센 규칙 글자별 수의 합이고 세기 전에는 null 이다`() {
        metricCollector.runOnce()
        val queue = Queue(listOf(alert("memory-1", "ci-agent-158")), emptyMap())
        val oneRound = round(queue)
        assertNull(skippedInHealth(health(oneRound)), "세기 전인데 수가 나왔다")
        queue.counting = { AlertCounts(5, 1, mapOf("log_silent" to 2L, "log_silent_all" to 1L)) }
        oneRound.dispatchOnce(now)
        val body = health(oneRound)
        assertEquals(3L, (skippedInHealth(body) as? Number)?.toLong(), "헬스에 모르는 줄의 수(규칙 글자별 수의 합)가 없다: $body")
        assertEquals(5L, (body["undelivered_alerts"] as Number).toLong())
        assertEquals("ok", body["status"], "모르는 줄이 지표 수집의 판정을 바꿨다")
        assertFalse(health(null).containsKey("skipped_unknown_rule_alerts"), "판정을 붙이지 않았는데 칸이 있다")
    }
}
