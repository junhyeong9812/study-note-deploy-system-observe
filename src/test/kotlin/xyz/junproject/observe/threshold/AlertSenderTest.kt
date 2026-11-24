package xyz.junproject.observe.threshold

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 알림 서버 `POST /events` 로 보내기 — 알림 서버의 받는 계약(`AlertController.postEvent`: 머리글 `X-Alert-Secret`, 본문
 * `{source, kind, severity, key, title, detail}`, 받으면 202)에 맞는가, 응답마다 다시 보낼지(Retry) 그만둘지(Rejected), 기한.
 * 알림 서버 대신 JDK 의 작은 HTTP 서버를 띄운다.
 */
class HttpAlertSenderTest {
    private class Received(val method: String, val path: String, val secret: String?, val contentType: String?, val body: String)

    private val received = CopyOnWriteArrayList<Received>()
    private var status = 202
    private var delayMillis = 0L
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            val body = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8)
            received += Received(exchange.requestMethod, exchange.requestURI.path, exchange.requestHeaders.getFirst("X-Alert-Secret"),
                exchange.requestHeaders.getFirst("Content-Type"), body)
            if (delayMillis > 0) Thread.sleep(delayMillis)
            val answer = """{"success":true,"data":{"sent":true,"key":"k"}}""".toByteArray()
            exchange.sendResponseHeaders(status, answer.size.toLong())
            exchange.responseBody.use { it.write(answer) }
        }
        executor = java.util.concurrent.Executors.newCachedThreadPool()
        start()
    }
    private val eventsUrl = "http://127.0.0.1:${server.address.port}/events"
    private val json = JsonMapper.builder().build()

    @AfterEach fun stop() = server.stop(0)

    private val alert = ResourceAlert(
        eventId = "resource:ci-agent-158:memory@1763953200000", key = "resource:ci-agent-158:memory", kind = "resource_memory",
        severity = AlertSeverity.WARNING, title = "ci-agent-158 메모리 93.5% — 5분 넘게 90% 이상", detail = "mem_used 15.0G / 16.0G",
        server = "ci-agent-158", rule = ResourceRule.MEMORY, targetName = null, createdAt = Instant.parse("2026-11-24T03:00:00Z"),
    )

    @Test
    fun `POST events 에 비밀값 머리글과 JSON 본문 여섯 칸을 보내고 202 면 Delivered`() {
        val outcome = HttpAlertSender(eventsUrl, "alert-secret").send(alert)
        assertEquals(SendOutcome.Delivered, outcome)
        val request = received.single()
        assertEquals("POST", request.method)
        assertEquals("/events", request.path)
        assertEquals("alert-secret", request.secret)
        assertTrue(request.contentType?.startsWith("application/json") == true, request.contentType)
        @Suppress("UNCHECKED_CAST")
        val body = json.readValue(request.body, Map::class.java) as Map<String, Any?>
        assertEquals(mapOf("source" to "observe", "kind" to "resource_memory", "severity" to "warning", "key" to "resource:ci-agent-158:memory",
            "title" to alert.title, "detail" to alert.detail), body)
    }

    @Test
    fun `2xx 는 모두 받은 것이다`() {
        for (code in listOf(200, 201, 202, 204)) {
            status = code
            assertEquals(SendOutcome.Delivered, HttpAlertSender(eventsUrl, "s").send(alert), "$code")
        }
    }

    @Test
    fun `5xx · 429 · 401 · 403 은 다시 보낸다 — 사유는 http 코드`() {
        for (code in listOf(500, 502, 503, 429, 401, 403)) {
            status = code
            assertEquals(SendOutcome.Retry("http $code"), HttpAlertSender(eventsUrl, "s").send(alert), "$code")
        }
    }

    @Test
    fun `그 밖의 4xx 는 본문 거절이라 다시 보내지 않는다`() {
        for (code in listOf(400, 404, 413, 422)) {
            status = code
            assertEquals(SendOutcome.Rejected("http $code"), HttpAlertSender(eventsUrl, "s").send(alert), "$code")
        }
    }

    @Test
    fun `닿지 못하면 예외 없이 Retry`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        val outcome = HttpAlertSender("http://127.0.0.1:$closedPort/events", "s").send(alert)
        assertTrue(outcome is SendOutcome.Retry, "$outcome")
        assertFalse((outcome as SendOutcome.Retry).reason.contains("127.0.0.1"), "사유에 주소를 넣었다: ${outcome.reason}")
    }

    @Test
    fun `응답이 기한보다 늦으면 기한에서 끊고 Retry`() {
        delayMillis = 3_000
        val started = System.nanoTime()
        val outcome = HttpAlertSender(eventsUrl, "s", timeout = Duration.ofMillis(300)).send(alert)
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(outcome is SendOutcome.Retry, "$outcome")
        assertTrue(elapsed < Duration.ofMillis(2_000), "기한을 지키지 않았다: $elapsed")
    }

    @Test
    fun `주소가 비어 있으면 보내지 않고 not_configured 로 남긴다`() {
        assertEquals(SendOutcome.Retry("not_configured"), HttpAlertSender("", "s").send(alert))
        assertEquals(SendOutcome.Retry("not_configured"), HttpAlertSender("   ", "s").send(alert))
        assertTrue(received.isEmpty())
    }

    @Test
    fun `기본 기한은 3초다 — ci-cd 의 보내는 쪽과 같다`() {
        delayMillis = 3_600
        val started = System.nanoTime()
        assertTrue(HttpAlertSender(eventsUrl, "s").send(alert) is SendOutcome.Retry)
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed >= Duration.ofMillis(2_500) && elapsed < Duration.ofMillis(3_500), "$elapsed")
    }
}

/** 보낼 표를 읽어 보내기 — 결과마다 표시, 보내는 쪽의 예외는 Retry, 잠금을 못 얻으면 보내지 않음, 오래된 것부터 */
class AlertDispatcherTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    /** 알림마다 다른 대상(서버) — "다시 시도 · 예외가 나도 다른 대상은 계속 보낸다"(§15.1 — 같은 대상의 뒤 알림은 그 바퀴에 멈춘다) */
    private fun alert(number: Int) = ResourceAlert("event-$number", "resource:ci-agent-$number:memory", "resource_memory", AlertSeverity.WARNING,
        "t$number", "d", "ci-agent-$number", ResourceRule.MEMORY, null, now.minusSeconds(100L - number))

    private class FakeQueue(var queued: MutableList<QueuedAlert>) : AlertQueue {
        val marks = mutableListOf<String>()
        var lockAvailable = true
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun markDelivered(id: Long, at: Instant) { marks += "delivered:$id"; queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { marks += "retry:$id:$reason" }
        override fun markRejected(id: Long, at: Instant, reason: String) { marks += "rejected:$id:$reason"; queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = if (lockAvailable) block() else null
        override fun alertCounts() = AlertCounts(queued.size.toLong(), 0)
    }

    private val queue = FakeQueue((1..4).map { QueuedAlert(it.toLong(), alert(it), 0, null) }.toMutableList())

    @Test
    fun `결과마다 표시하고 보내는 쪽의 예외도 Retry 로 보아 나머지를 계속 보낸다`() {
        val sent = mutableListOf<String>()
        val sender = object : AlertSender {
            override fun send(alert: ResourceAlert): SendOutcome {
                sent += alert.eventId
                return when (alert.eventId) {
                    "event-1" -> SendOutcome.Delivered
                    "event-2" -> SendOutcome.Retry("http 503")
                    "event-3" -> throw IllegalStateException("보내는 쪽 고장")
                    else -> SendOutcome.Rejected("http 422")
                }
            }
        }
        val outcome = AlertDispatcher(queue, sender).dispatch(now)
        assertEquals(listOf("event-1", "event-2", "event-3", "event-4"), sent, "오래된 것부터 모두 보내야 한다")
        assertEquals(DispatchOutcome(true, 1, 2, 1), outcome)
        assertEquals(listOf("delivered:1", "retry:2:http 503", "retry:3:IllegalStateException", "rejected:4:http 422"), queue.marks)
    }

    @Test
    fun `보내기 잠금을 얻지 못하면 아무것도 보내지 않는다`() {
        queue.lockAvailable = false
        val sender = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome = fail("잠금 없이 보냈다") }
        assertEquals(DispatchOutcome(false, 0, 0, 0), AlertDispatcher(queue, sender).dispatch(now))
        assertTrue(queue.marks.isEmpty())
    }

    @Test
    fun `한 바퀴에 묶음 크기까지만 보낸다`() {
        val sent = mutableListOf<String>()
        val sender = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return SendOutcome.Delivered } }
        assertEquals(DispatchOutcome(true, 2, 0, 0), AlertDispatcher(queue, sender, batchSize = 2).dispatch(now))
        assertEquals(listOf("event-1", "event-2"), sent)
    }
}
