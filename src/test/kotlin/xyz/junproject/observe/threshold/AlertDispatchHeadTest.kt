package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 명세 §16 · §16.1 (리뷰 루프 2) — 보내기는 대상마다 줄의 머리만: 머리가 전달 · 거절이면 같은 바퀴에 그 대상의 다음 건, "다시 시도"면 그 대상은 끝,
 * 대상의 차례는 마지막 시도가 가장 오래된 대상부터(시도한 적 없는 대상이 맨 앞), 한 건의 기한(3초)을 더해 30초를 넘게 되면 새로 보내지 않는다,
 * 디스크 경고와 위험은 한 대상. 보낼 표는 [AlertQueue.pendingByTarget] 을 따르는 가짜.
 */
class AlertDispatchHeadTest {
    private val start = Instant.parse("2026-11-24T03:00:00Z")
    private var clock = start
    private var sequence = 0

    private fun alert(eventId: String, server: String = "ci-agent-158", rule: ResourceRule = ResourceRule.MEMORY, key: String = "resource:$server:${rule.wireName}") =
        ResourceAlert(eventId, key, rule.kind, AlertSeverity.WARNING, "t", "d", server, rule, null, start.plusMillis((sequence++).toLong()))

    private class Queue(alerts: List<ResourceAlert>) : AlertQueue {
        val queued = alerts.mapIndexed { index, alert -> QueuedAlert(index + 1L, alert, 0, null) }.toMutableList()
        val lastAttempt = mutableMapOf<AlertTarget, Instant>()
        private fun attempted(id: Long, at: Instant) { lastAttempt[targetOf(queued.first { it.id == id }.alert)] = at }
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun pendingByTarget(perTarget: Int) = targetQueues(queued, lastAttempt, perTarget)
        override fun markDelivered(id: Long, at: Instant) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { attempted(id, at) }
        override fun markRejected(id: Long, at: Instant, reason: String) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = block()
        override fun alertCounts() = AlertCounts(queued.size.toLong(), 0)
    }

    /** 보낼 때마다 [perSend] 만큼 시계가 흐르고 [outcome] 을 돌려주는 보내는 쪽 */
    private inner class Sender(private val perSend: Duration = Duration.ZERO, private val outcome: (ResourceAlert) -> SendOutcome) : AlertSender {
        val sent = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; clock = clock.plus(perSend); return outcome(alert) }
    }

    private fun dispatcher(queue: Queue, sender: Sender) = AlertDispatcher(queue, sender, clock = { clock })

    @Test
    fun `머리가 전달되면 같은 바퀴에 그 대상의 다음 건이 이어서 간다`() {
        val queue = Queue(listOf(alert("memory-1"), alert("memory-2"), alert("memory-3")))
        val sender = Sender { SendOutcome.Delivered }
        assertEquals(DispatchOutcome(true, 3, 0, 0), dispatcher(queue, sender).dispatch(start))
        assertEquals(listOf("memory-1", "memory-2", "memory-3"), sender.sent)
    }

    @Test
    fun `머리가 다시 시도면 그 대상은 이번 바퀴에 더 보내지 않고 거절이면 다음 건으로`() {
        val queue = Queue(listOf(alert("memory-1"), alert("memory-2"), alert("cpu-1", rule = ResourceRule.CPU), alert("cpu-2", rule = ResourceRule.CPU)))
        val sender = Sender { when (it.eventId) { "memory-1" -> SendOutcome.Retry("http 503"); "cpu-1" -> SendOutcome.Rejected("http 422"); else -> SendOutcome.Delivered } }
        dispatcher(queue, sender).dispatch(start)
        assertFalse("memory-2" in sender.sent, "다시 시도가 된 대상의 다음 건을 보냈다: ${sender.sent}")
        assertTrue("cpu-2" in sender.sent, "거절 뒤에 그 대상의 다음 건을 보내지 않았다: ${sender.sent}")
        assertEquals(listOf("memory-1", "memory-2"), queue.queued.map { it.alert.eventId })
    }

    @Test
    fun `대상의 차례는 마지막 시도가 가장 오래된 대상부터 — 앞쪽이 시간을 다 써도 다음 바퀴에 뒤쪽이 먼저 간다`() {
        val queue = Queue((1..5).map { alert("target-$it", server = "server-$it") })
        val sender = Sender(Duration.ofSeconds(10)) { SendOutcome.Retry("http 503") }
        val rounds = (0 until 3).map { round ->
            val before = sender.sent.size
            dispatcher(queue, sender).dispatch(clock)
            sender.sent.drop(before).also { clock = clock.plusSeconds(30) }
        }
        assertEquals(listOf("target-1", "target-2", "target-3"), rounds[0])
        assertEquals(listOf("target-4", "target-5", "target-1"), rounds[1], "시도한 적 없는 대상이 먼저 가야 한다")
        assertEquals(listOf("target-2", "target-3", "target-4"), rounds[2], "방금 다시 시도가 된 대상이 맨 뒤로 가지 않았다")
    }

    @Test
    fun `한 건의 기한을 더하면 30초를 넘게 되는 때에는 새로 보내지 않아 바퀴 전체가 30초를 넘지 않는다`() {
        for ((perSend, expected) in listOf(Duration.ofSeconds(3) to 10, Duration.ofSeconds(4) to 7)) {
            clock = start
            val queue = Queue((1..12).map { alert("alert-$it", server = "server-$it") })
            val sender = Sender(perSend) { SendOutcome.Delivered }
            dispatcher(queue, sender).dispatch(start)
            assertEquals(expected, sender.sent.size, "한 건 ${perSend.seconds}초")
            assertTrue(Duration.between(start, clock) <= Duration.ofSeconds(30), "바퀴가 30초를 넘었다: ${Duration.between(start, clock)}")
        }
    }

    @Test
    fun `디스크 경고와 위험은 한 대상 — 위험이 다시 시도로 남으면 그 서버의 위험 해소 · 경고가 먼저 가지 않고 메모리는 따로 간다`() {
        val queue = Queue(listOf(
            alert("critical", rule = ResourceRule.DISK_CRITICAL),
            alert("critical-resolved", rule = ResourceRule.DISK_CRITICAL, key = "resource:ci-agent-158:disk_critical:resolved"),
            alert("warning", rule = ResourceRule.DISK_WARNING),
            alert("memory"),
        ))
        val sender = Sender { if (it.eventId == "critical") SendOutcome.Retry("http 503") else SendOutcome.Delivered }
        dispatcher(queue, sender).dispatch(start)
        assertEquals(listOf("critical", "memory"), sender.sent)
        assertEquals(AlertTarget("ci-agent-158", "disk", null), AlertTarget.of(queue.queued.first().alert), "디스크 두 규칙이 한 묶음이 아니다")
        assertEquals(AlertTarget.of(queue.queued[0].alert), AlertTarget.of(queue.queued[2].alert))
    }

    @Test
    fun `대상은 서버 · 규칙 · 대상 이름이고 재시작은 컨테이너마다 다르다`() {
        val green = alert("green", rule = ResourceRule.RESTARTS).copy(targetName = "front-app-green")
        assertEquals(AlertTarget("ci-agent-158", "restarts", "front-app-green"), AlertTarget.of(green))
        assertEquals(AlertTarget("ci-agent-158", "memory", null), AlertTarget.of(alert("m")))
        assertNotEquals(AlertTarget.of(green), AlertTarget.of(green.copy(targetName = "front-app-blue")))
    }
}
