package xyz.junproject.observe.threshold

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * 자원 알림이 알림 서버로 가는 본문의 바이트 — 지금의 꼴을 그대로 고정한다(알림 타입을 넓혀도 자원 알림의 몸은 한 바이트도 바뀌지 않는다):
 * 칸의 차례(source · kind · severity · key · title · detail) · 한글은 UTF-8 그대로 · 줄바꿈과 따옴표는 JSON 이스케이프 · title 200자 · detail 1500자에서 자름.
 * 알림 서버 대신 JDK 의 작은 HTTP 서버가 받은 바이트를 본다.
 */
class AlertEventBodyBytesTest {
    private val received = CopyOnWriteArrayList<ByteArray>()
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            received += exchange.requestBody.readAllBytes()
            exchange.sendResponseHeaders(202, -1)
            exchange.close()
        }
        start()
    }
    private val eventsUrl = "http://127.0.0.1:${server.address.port}/events"

    @AfterEach fun stop() = server.stop(0)

    private fun sentBytes(alert: ResourceAlert): ByteArray {
        assertEquals(SendOutcome.Delivered, HttpAlertSender(eventsUrl, "alert-secret").send(alert))
        return received.single()
    }

    @Test
    fun `자원 알림의 본문은 칸의 차례와 이스케이프까지 지금의 바이트 그대로다`() {
        val alert = ResourceAlert(
            eventId = "resource:ci-agent-158:memory@1763953200000", key = "resource:ci-agent-158:memory", kind = "resource_memory",
            severity = AlertSeverity.WARNING, title = "ci-agent-158 메모리 93.5% — 5분 넘게 90% 이상",
            detail = "최근 5분 의 최저 91.0% · 최고 95.5% (표본 10개)\n처음 넘은 시각: 2026-11-24T03:00:00Z \"따옴표\"",
            server = "ci-agent-158", rule = ResourceRule.MEMORY, targetName = null, createdAt = Instant.parse("2026-11-24T03:00:00Z"),
        )
        val expected = """{"source":"observe","kind":"resource_memory","severity":"warning","key":"resource:ci-agent-158:memory",""" +
            """"title":"ci-agent-158 메모리 93.5% — 5분 넘게 90% 이상",""" +
            """"detail":"최근 5분 의 최저 91.0% · 최고 95.5% (표본 10개)\n처음 넘은 시각: 2026-11-24T03:00:00Z \"따옴표\""}"""
        assertArrayEquals(expected.toByteArray(Charsets.UTF_8), sentBytes(alert), "자원 알림의 본문 바이트가 바뀌었다: ${received.single().toString(Charsets.UTF_8)}")
    }

    @Test
    fun `자원 알림의 title 은 200자 · detail 은 1500자에서 잘려 나가고 재시작 규칙 · 위험 등급의 글자도 그대로다`() {
        val alert = ResourceAlert(
            eventId = "e", key = "resource:ci-agent-9:restarts:front-app-green", kind = "resource_restarts", severity = AlertSeverity.CRITICAL,
            title = "가".repeat(250), detail = "d".repeat(1_600), server = "ci-agent-9", rule = ResourceRule.RESTARTS, targetName = "front-app-green",
            createdAt = Instant.parse("2026-11-24T03:00:00Z"),
        )
        val expected = """{"source":"observe","kind":"resource_restarts","severity":"critical","key":"resource:ci-agent-9:restarts:front-app-green",""" +
            """"title":"${"가".repeat(200)}","detail":"${"d".repeat(1_500)}"}"""
        assertArrayEquals(expected.toByteArray(Charsets.UTF_8), sentBytes(alert))
    }
}
