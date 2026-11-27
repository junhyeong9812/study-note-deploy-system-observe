package xyz.junproject.observe.threshold

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import tools.jackson.databind.json.JsonMapper
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * ⑬ T4b · 통신 규약 §2.1 · §2.2 · §7.1 의 T4 "앱 여섯" 줄 — 관측 서버가 **우리 서비스**를 부르는 호출에 자기 규약 이름을
 * `X-Caller-Service: observe` 로 싣는다. 받는 쪽 nginx 가 이 머리글로 접속 기록의 `caller` 를 적는다(`map … $protocol_caller` — 값이 정확히
 * `observe` 일 때만 `observe`, 그 밖은 `outside`).
 * - 관측 서버가 우리 서비스로 내는 HTTP 호출은 알림 서버 `POST /events`([HttpAlertSender]) 하나다 — 자원 알림 · 로그 알림이 같은 보낼 표와 같은 보내는 쪽을 쓴다.
 * - 머리글은 **정확히 하나** · 값은 `observe`(소문자 · 코드의 상수). 첫 호출도, 다음 바퀴에 같은 알림을 다시 보내는 호출도.
 * - 들어온 요청의 `X-Caller-Service` 를 옮겨 싣지 않는다 · 설정(`SERVER_NAME` — 테스트 환경에서는 `front`)에서 읽지 않는다.
 * - 그 밖의 머리글(요청 ID · 비밀값 · Content-Type) · 본문 · 결과 판정은 그대로.
 * 알림 서버 대신 JDK 의 작은 HTTP 서버를 띄운다 — 받은 머리글을 그대로 본다.
 */
class AlertSenderCallerServiceTest {
    private class Received(val callers: List<String>, val requestIds: List<String>, val secret: String?, val contentType: String?, val body: String)

    private val received = CopyOnWriteArrayList<Received>()
    private var status = 202
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange ->
            val body = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8)
            received += Received(exchange.requestHeaders["X-Caller-Service"].orEmpty(), exchange.requestHeaders["X-Request-Id"].orEmpty(),
                exchange.requestHeaders.getFirst("X-Alert-Secret"), exchange.requestHeaders.getFirst("Content-Type"), body)
            val answer = """{"success":true}""".toByteArray()
            exchange.sendResponseHeaders(status, answer.size.toLong())
            exchange.responseBody.use { it.write(answer) }
        }
        executor = Executors.newCachedThreadPool()
        start()
    }
    private val eventsUrl = "http://127.0.0.1:${server.address.port}/events"
    private val json = JsonMapper.builder().build()

    @AfterEach fun stop() = server.stop(0)

    private fun alert(eventId: String = "resource:ci-agent-158:memory@1763953200000", key: String = "resource:ci-agent-158:memory") = ResourceAlert(
        eventId = eventId, key = key, kind = "resource_memory",
        severity = AlertSeverity.WARNING, title = "ci-agent-158 메모리 93.5% — 5분 넘게 90% 이상", detail = "mem_used 15.0G / 16.0G",
        server = "ci-agent-158", rule = ResourceRule.MEMORY, targetName = null, createdAt = Instant.parse("2026-11-24T03:00:00Z"),
    )

    private fun assertCallerIsObserve(request: Received, what: String) {
        assertEquals(listOf("observe"), request.callers, "$what: X-Caller-Service 가 정확히 하나의 observe 가 아니다")
    }

    private fun assertRequestIdKept(request: Received, what: String) {
        assertEquals(1, request.requestIds.size, "$what: 요청 ID 머리글 수가 바뀌었다 ${request.requestIds}")
        assertTrue(Regex("^[0-9a-f]{32}$").matches(request.requestIds.single()), "$what: 요청 ID 의 꼴이 바뀌었다 ${request.requestIds.single()}")
    }

    // ---- 싣는다: 정확히 하나 · observe ----

    @Test
    fun `알림 서버로 보내는 요청은 X-Caller-Service observe 를 하나 싣고 요청 ID 와 결과는 그대로다`() {
        assertEquals(SendOutcome.Delivered, HttpAlertSender(eventsUrl, "alert-secret").send(alert()), "결과가 바뀌었다")
        val request = received.single()
        assertCallerIsObserve(request, "첫 호출")
        assertRequestIdKept(request, "첫 호출")
    }

    @Test
    fun `같은 알림을 다시 보낼 때도 · 다른 알림도 · 보내는 곳을 새로 만들어도 매번 하나의 observe 다`() {
        val sender = HttpAlertSender(eventsUrl, "s")
        repeat(3) { sender.send(alert()) }                                                    // 같은 알림을 다음 바퀴마다 다시 보냄
        sender.send(alert(eventId = "resource:ci-agent-158:disk@1763953200000", key = "resource:ci-agent-158:disk"))
        HttpAlertSender(eventsUrl, "s").send(alert())
        assertEquals(5, received.size, "준비: 다섯 번 보내지 않았다")
        received.forEachIndexed { index, request -> assertCallerIsObserve(request, "${index + 1}번째 호출") }
    }

    @Test
    fun `다시 보내라는 답 · 거절의 답을 받는 요청에도 싣고 결과 판정은 그대로다`() {
        val sender = HttpAlertSender(eventsUrl, "s")
        val outcomes = listOf(503, 429, 401, 400).map { code -> status = code; sender.send(alert()) }
        assertEquals(listOf(SendOutcome.Retry("http 503"), SendOutcome.Retry("http 429"), SendOutcome.Retry("http 401"), SendOutcome.Rejected("http 400")),
            outcomes, "결과 판정이 바뀌었다")
        assertEquals(4, received.size, "준비")
        received.forEachIndexed { index, request -> assertCallerIsObserve(request, "응답 ${listOf(503, 429, 401, 400)[index]}") }
    }

    @Test
    fun `보내기 바퀴가 처음에 받지 못한 알림을 다음 바퀴에 다시 보낼 때도 하나의 observe 다`() {
        val queue = OneTargetQueue(listOf(alert()))
        val dispatcher = AlertDispatcher(queue, HttpAlertSender(eventsUrl, "s"))
        status = 503
        assertEquals(DispatchOutcome(true, 0, 1, 0), dispatcher.dispatch(Instant.now()), "준비: 첫 바퀴가 다시 시도가 아니다")
        status = 202
        assertEquals(DispatchOutcome(true, 1, 0, 0), dispatcher.dispatch(Instant.now()), "준비: 둘째 바퀴가 전달이 아니다")
        assertEquals(2, received.size, "준비: 두 번 보내지 않았다")
        assertCallerIsObserve(received[0], "첫 시도")
        assertCallerIsObserve(received[1], "다시 보낸 호출")
    }

    @Test
    fun `그 밖의 머리글과 본문은 그대로이고 본문에 호출자를 싣지 않는다`() {
        HttpAlertSender(eventsUrl, "alert-secret").send(alert())
        val request = received.single()
        assertEquals("alert-secret", request.secret)
        assertTrue(request.contentType?.startsWith("application/json") == true, request.contentType)
        @Suppress("UNCHECKED_CAST")
        assertEquals(alert().eventBody(), json.readValue(request.body, Map::class.java) as Map<String, Any?>, "본문이 달라졌다")
        assertFalse(request.body.contains("caller", ignoreCase = true), "본문에 호출자 칸을 실었다: ${request.body}")
    }

    // ---- 들어온 값을 옮기지 않는다 · 설정에서 읽지 않는다 ----

    /** 들어온 요청 안에서 알림을 보내는 이 테스트만의 컨트롤러 — 들어온 요청의 문맥(머리글)이 보내는 쪽에 보이는 자리 */
    @RestController
    class CallingController(private val sender: AlertSender, private val alert: ResourceAlert) {
        @GetMapping("/test/send-alert")
        fun call(): String = sender.send(alert).toString()
    }

    @Test
    fun `들어온 요청의 X-Caller-Service 를 옮겨 싣지 않고 자기 이름을 싣는다`() {
        val incomingValues = listOf("admin", "front", "edge", "outside", "OBSERVE", "observe, admin")
        val mockMvc = MockMvcBuilders.standaloneSetup(CallingController(HttpAlertSender(eventsUrl, "s"), alert())).build()
        incomingValues.forEach { incoming ->
            val response = mockMvc.perform(get("/test/send-alert").header("X-Caller-Service", incoming)).andReturn().response
            assertEquals(200, response.status, "준비: $incoming")
        }
        assertEquals(incomingValues.size, received.size, "준비: 들어온 요청마다 한 번 보내지 않았다")
        received.forEachIndexed { index, request -> assertCallerIsObserve(request, "들어온 값 ${incomingValues[index]}") }
    }

    @Test
    fun `같은 스레드에 들어온 요청의 문맥이 남아 있어도 자기 이름을 싣는다`() {
        val incoming = MockHttpServletRequest("GET", "/metrics/latest").apply { addHeader("X-Caller-Service", "admin") }
        RequestContextHolder.setRequestAttributes(ServletRequestAttributes(incoming))
        try {
            HttpAlertSender(eventsUrl, "s").send(alert())
        } finally {
            RequestContextHolder.resetRequestAttributes()
        }
        assertCallerIsObserve(received.single(), "요청 문맥이 admin")
    }

    @Test
    fun `설정의 이름을 다르게 두어도 observe 다`() {
        assertEquals("front", System.getenv("SERVER_NAME"), "준비: 테스트의 환경변수 SERVER_NAME 이 front 가 아니다(build.gradle.kts)")
        val previous = System.getProperty("SERVER_NAME")
        System.setProperty("SERVER_NAME", "front")
        try {
            HttpAlertSender(eventsUrl, "s").send(alert())
        } finally {
            if (previous == null) System.clearProperty("SERVER_NAME") else System.setProperty("SERVER_NAME", previous)
        }
        assertCallerIsObserve(received.single(), "SERVER_NAME=front")
    }

    /** 대상 하나의 보낼 표 — 전달 · 거절이면 빠지고 다시 시도면 남는다 */
    private class OneTargetQueue(alerts: List<ResourceAlert>) : AlertQueue {
        val queued = alerts.mapIndexed { index, alert -> QueuedAlert(index + 1L, alert, 0, null) }.toMutableList()
        private val lastAttempt = mutableMapOf<AlertTarget, Instant>()
        private fun attempted(id: Long, at: Instant) { lastAttempt[targetOf(queued.first { it.id == id }.alert)] = at }
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun pendingByTarget(perTarget: Int) = targetQueues(queued, lastAttempt, perTarget)
        override fun markDelivered(id: Long, at: Instant) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { attempted(id, at) }
        override fun markRejected(id: Long, at: Instant, reason: String) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = block()
        override fun alertCounts() = AlertCounts(queued.size.toLong(), 0)
    }
}
