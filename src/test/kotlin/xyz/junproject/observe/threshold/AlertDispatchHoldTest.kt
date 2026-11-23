package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 명세 §15 · §15.1 (리뷰 루프 1) — 보내기가 멈추는 범위: "다시 시도"가 나온 알림과 같은 대상(서버 · 규칙 · 대상)의 뒤 알림만 그 바퀴에
 * 보내지 않고 다른 대상은 계속 보낸다 · 한 건이 매번 5xx 여도 나머지는 전달된다 · "거절"은 건너뛰고 계속 · 한 바퀴는 합쳐 30초를 넘기면 멈춘다.
 */
class AlertDispatchHoldTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    private fun alert(eventId: String, server: String = "ci-agent-158", rule: ResourceRule = ResourceRule.MEMORY, targetName: String? = null) =
        ResourceAlert(eventId, "resource:$server:${rule.wireName}${targetName?.let { ":$it" } ?: ""}", rule.kind, AlertSeverity.WARNING,
            "t", "d", server, rule, targetName, now)

    /** 보낼 표 흉내 — 받은 것 · 거절된 것은 빠지고 다시 시도는 남는다(오래된 것부터) */
    private class FakeQueue(alerts: List<ResourceAlert>) : AlertQueue {
        val queued = alerts.mapIndexed { index, alert -> QueuedAlert(index + 1L, alert, 0, null) }.toMutableList()
        val marks = mutableListOf<String>()
        private fun idOf(id: Long) = queued.first { it.id == id }.alert.eventId
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun markDelivered(id: Long, at: Instant) { marks += "delivered:${idOf(id)}"; queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { marks += "retry:${idOf(id)}" }
        override fun markRejected(id: Long, at: Instant, reason: String) { marks += "rejected:${idOf(id)}"; queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = block()
        override fun alertCounts() = AlertCounts(queued.size.toLong(), 0)
    }

    private class ScriptedSender(private val outcome: (ResourceAlert) -> SendOutcome) : AlertSender {
        val sent = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return outcome(alert) }
    }

    @Test
    fun `다시 시도가 나온 대상의 뒤 알림은 그 바퀴에 보내지 않고 다른 대상은 계속 보낸다`() {
        val queue = FakeQueue(listOf(
            alert("memory-1"), alert("cpu-1", rule = ResourceRule.CPU), alert("memory-2"), alert("other-server-memory", server = "ci-agent-164"),
            alert("memory-3"),
        ))
        val sender = ScriptedSender { if (it.eventId == "memory-1") SendOutcome.Retry("http 503") else SendOutcome.Delivered }
        val outcome = AlertDispatcher(queue, sender).dispatch(now)
        assertEquals(listOf("memory-1", "cpu-1", "other-server-memory"), sender.sent, "같은 대상의 뒤 알림을 보냈거나 다른 대상을 막았다")
        assertEquals(listOf("memory-1", "memory-2", "memory-3"), queue.queued.map { it.alert.eventId }, "같은 대상의 순서가 지켜지지 않았다")
        assertEquals(DispatchOutcome(true, 2, 1, 0), outcome)
        assertFalse(queue.marks.any { it.endsWith("memory-2") || it.endsWith("memory-3") }, "보내지 않은 알림을 표시했다: ${queue.marks}")
    }

    @Test
    fun `재시작 규칙은 컨테이너가 다르면 다른 대상이다`() {
        val queue = FakeQueue(listOf(
            alert("green-1", rule = ResourceRule.RESTARTS, targetName = "front-app-green"),
            alert("blue-1", rule = ResourceRule.RESTARTS, targetName = "front-app-blue"),
            alert("green-2", rule = ResourceRule.RESTARTS, targetName = "front-app-green"),
        ))
        val sender = ScriptedSender { if (it.eventId == "green-1") SendOutcome.Retry("http 503") else SendOutcome.Delivered }
        AlertDispatcher(queue, sender).dispatch(now)
        assertEquals(listOf("green-1", "blue-1"), sender.sent)
    }

    @Test
    fun `한 건이 매번 5xx 여도 나머지 대상은 바퀴마다 전달된다`() {
        val queue = FakeQueue(listOf(alert("stuck-1")) + (1..6).map { alert("cpu-$it", server = "server-$it", rule = ResourceRule.CPU) } + alert("stuck-2"))
        val sender = ScriptedSender { if (it.eventId.startsWith("stuck")) SendOutcome.Retry("http 500") else SendOutcome.Delivered }
        repeat(3) { AlertDispatcher(queue, sender).dispatch(now.plusSeconds(30L * it)) }
        assertEquals(listOf("stuck-1", "stuck-2"), queue.queued.map { it.alert.eventId }, "막힌 한 건이 다른 대상의 전달을 막았다")
        assertEquals(3, sender.sent.count { it == "stuck-1" })
        assertEquals(0, sender.sent.count { it == "stuck-2" }, "같은 대상의 순서를 건너뛰었다")
    }

    @Test
    fun `거절은 건너뛰고 같은 대상의 뒤 알림도 계속 보낸다`() {
        val queue = FakeQueue(listOf(alert("memory-1"), alert("memory-2"), alert("cpu-1", rule = ResourceRule.CPU)))
        val sender = ScriptedSender { if (it.eventId == "memory-1") SendOutcome.Rejected("http 422") else SendOutcome.Delivered }
        assertEquals(DispatchOutcome(true, 2, 0, 1), AlertDispatcher(queue, sender).dispatch(now))
        assertEquals(listOf("memory-1", "memory-2", "cpu-1"), sender.sent)
    }

    /** 보낼 때마다 [perSend] 만큼 흐르는 가짜 시계 */
    private fun budgetRun(perSend: Duration): Pair<List<String>, FakeQueue> {
        var clock = now
        val queue = FakeQueue((1..6).map { alert("alert-$it", server = "server-$it") })
        val sender = ScriptedSender { clock = clock.plus(perSend); SendOutcome.Delivered }
        AlertDispatcher(queue, sender, clock = { clock }).dispatch(now)
        return sender.sent to queue
    }

    @Test
    fun `한 바퀴의 보내기가 합쳐 30초를 넘기면 거기서 멈추고 남은 것은 다음 바퀴로`() {
        val (sent, queue) = budgetRun(Duration.ofSeconds(11))
        assertEquals(listOf("alert-1", "alert-2", "alert-3"), sent, "33초가 지났는데 계속 보냈거나 30초 전에 멈췄다")
        assertEquals(listOf("alert-4", "alert-5", "alert-6"), queue.queued.map { it.alert.eventId })
    }

    @Test
    fun `정확히 30초는 넘긴 것이 아니다`() {
        assertEquals(listOf("alert-1", "alert-2", "alert-3", "alert-4"), budgetRun(Duration.ofSeconds(10)).first)
    }
}
