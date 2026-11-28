package xyz.junproject.observe.threshold

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * 통신 규약 §1.2 "들어온 요청 없이 시작되는 호출" · §7.1 의 T2 observe 줄 — 관측 서버가 알림 서버로 보내는 `POST /events` 는
 * 들어온 요청 없이 스스로 시작하므로 **보낼 때마다 새 추적 ID** 를 머리글 `X-Request-Id` 하나로 싣는다(받는 쪽 nginx 의 접속 기록이 그 값을 남긴다).
 * 꼴은 16진수 소문자 32자(nginx 의 `$request_id` · alert 의 주기 호출과 같다). 같은 알림을 다음 바퀴에 다시 보내는 것도 새 호출 — 새 값이다.
 * 기존 머리글(비밀값 · Content-Type)과 본문은 그대로다. 알림 서버 대신 JDK 의 작은 HTTP 서버를 띄운다.
 */
class AlertSenderRequestIdTest {
    private class Received(val requestIds: List<String>, val secret: String?, val contentType: String?, val body: String)

    private val received = CopyOnWriteArrayList<Received>()
    private var status = 202
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            val body = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8)
            received += Received(exchange.requestHeaders["X-Request-Id"].orEmpty(), exchange.requestHeaders.getFirst("X-Alert-Secret"),
                exchange.requestHeaders.getFirst("Content-Type"), body)
            val answer = """{"success":true}""".toByteArray()
            exchange.sendResponseHeaders(status, answer.size.toLong())
            exchange.responseBody.use { it.write(answer) }
        }
        executor = Executors.newCachedThreadPool()
        start()
    }
    private val eventsUrl = "http://127.0.0.1:${server.address.port}/events"
    private val json = JsonMapper.builder().build()
    private val createdForm = Regex("^[0-9a-f]{32}$")

    @AfterEach fun stop() = server.stop(0)

    private fun alert(eventId: String = "resource:ci-agent-158:memory@1763953200000") = ResourceAlert(
        eventId = eventId, key = "resource:ci-agent-158:memory", kind = "resource_memory",
        severity = AlertSeverity.WARNING, title = "ci-agent-158 메모리 93.5% — 5분 넘게 90% 이상", detail = "mem_used 15.0G / 16.0G",
        server = "ci-agent-158", rule = ResourceRule.MEMORY, targetName = null, createdAt = Instant.parse("2026-11-24T03:00:00Z"),
    )

    /** 받은 요청 하나에 머리글이 꼭 하나 있고 꼴이 맞는지 본 뒤 그 값을 돌려준다 */
    private fun requestIdOf(request: Received): String {
        assertEquals(1, request.requestIds.size, "X-Request-Id 머리글 수: ${request.requestIds}")
        val requestId = request.requestIds.single()
        assertTrue(createdForm.matches(requestId), "16진수 소문자 32자가 아니다: $requestId")
        return requestId
    }

    @Test
    fun `알림 서버로 보내는 요청은 X-Request-Id 를 하나 싣고 꼴은 16진수 소문자 32자다`() {
        assertEquals(SendOutcome.Delivered, HttpAlertSender(eventsUrl, "alert-secret").send(alert()))
        requestIdOf(received.single())
    }

    @Test
    fun `보낼 때마다 새 값이다 — 같은 알림을 다시 보낼 때도 · 다른 알림 사이도 · 보내는 곳을 새로 만들어도`() {
        val sender = HttpAlertSender(eventsUrl, "s")
        repeat(3) { sender.send(alert()) }                                                    // 같은 알림을 다음 바퀴마다 다시 보냄
        sender.send(alert(eventId = "resource:ci-agent-158:disk@1763953200000"))
        HttpAlertSender(eventsUrl, "s").send(alert())
        val requestIds = received.map { requestIdOf(it) }
        assertEquals(5, requestIds.size, "준비: 다섯 번 보내지 않았다")
        assertEquals(5, requestIds.toSet().size, "같은 ID 를 다시 썼다: $requestIds")
    }

    @Test
    fun `다시 보내라는 답 · 거절의 답을 받는 요청에도 싣는다`() {
        val sender = HttpAlertSender(eventsUrl, "s")
        for (code in listOf(503, 429, 400)) {
            status = code
            sender.send(alert())
        }
        assertEquals(3, received.map { requestIdOf(it) }.toSet().size)
    }

    @Test
    fun `기존 머리글과 본문은 그대로다`() {
        HttpAlertSender(eventsUrl, "alert-secret").send(alert())
        val request = received.single()
        assertEquals("alert-secret", request.secret)
        assertTrue(request.contentType?.startsWith("application/json") == true, request.contentType)
        @Suppress("UNCHECKED_CAST")
        assertEquals(alert().eventBody(), json.readValue(request.body, Map::class.java) as Map<String, Any?>, "본문이 달라졌다")
        assertFalse(request.body.contains(requestIdOf(request)), "추적 ID 를 본문에 실었다 — 본문의 칸은 알림 서버의 계약 그대로여야 한다")
    }

    @Test
    fun `알림 서버에 닿지 못해도 예외 없이 Retry 다 — 머리글을 만드는 일이 보내기를 깨지 않는다`() {
        val closedPort = ServerSocket(0).use { it.localPort }
        val outcome = HttpAlertSender("http://127.0.0.1:$closedPort/events", "s").send(alert())
        assertTrue(outcome is SendOutcome.Retry, "$outcome")
    }
}
