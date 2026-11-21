package xyz.junproject.observe.api

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogCount
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogQuery
import xyz.junproject.observe.log.LogReader
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/** 명세 ②: 조회는 `X-Observe-Secret` 없으면 401, 조건이 틀리면 422. 기간을 안 주면 최근 1시간, 최대 500건, 커서 이어 보기. */
class LogControllerTest {
    private val now = Instant.parse("2026-11-21T12:00:00Z")
    private val received = mutableListOf<LogQuery>()
    private var answer: (LogQuery) -> List<LogEntry> = { emptyList() }
    private val reader = object : LogReader {
        override fun search(query: LogQuery): List<LogEntry> { received += query; return answer(query) }
        override fun stats(from: Instant, to: Instant): List<LogCount> =
            listOf(LogCount(from, "backend", "error", 3))
    }
    private val stream = FakeLogStream().also { it.ensureGroup() }
    private val collector = LogCollector(stream, FakeLogSink(), block = Duration.ZERO)
    private val health = CollectorHealth(collector, { stream.unacknowledgedCount() }, {})
    private val controller = LogController(reader, health, "the-secret")

    private fun request(secret: String? = "the-secret") = MockHttpServletRequest().apply { if (secret != null) addHeader("X-Observe-Secret", secret) }

    private fun logs(
        secret: String? = "the-secret", from: String? = null, to: String? = null, service: String? = null, level: String? = null,
        requestId: String? = null, q: String? = null, before: String? = null, limit: String? = null,
    ) = controller.logs(request(secret), from, to, service, level, requestId, q, before, limit, now)

    @Suppress("UNCHECKED_CAST")
    private fun data(response: org.springframework.http.ResponseEntity<out Any>) = (response.body as Map<String, Any>)["data"] as Map<String, Any>

    @Test
    fun `비밀값이 없거나 틀리면 401 이고 조회하지 않는다`() {
        assertEquals(401, logs(secret = null).statusCode.value())
        assertEquals(401, logs(secret = "wrong").statusCode.value())
        assertEquals(401, logs(secret = "").statusCode.value())
        assertEquals(401, controller.stats(request(null), null, null, now).statusCode.value())
        assertTrue(received.isEmpty())
    }

    @Test
    fun `설정된 비밀값이 비어 있으면 빈 헤더로도 열리지 않는다`() {
        val unconfigured = LogController(reader, health, "")
        assertEquals(401, unconfigured.logs(request(""), null, null, null, null, null, null, null, null, now).statusCode.value())
        assertEquals(401, unconfigured.logs(request(null), null, null, null, null, null, null, null, null, now).statusCode.value())
    }

    @Test
    fun `기간을 주지 않으면 최근 1시간을 찾는다`() {
        assertEquals(200, logs().statusCode.value())
        assertEquals(now.minus(Duration.ofHours(1)), received.single().from)
        assertEquals(now, received.single().to)
    }

    @Test
    fun `기간이 거꾸로면 422`() {
        assertEquals(422, logs(from = "2026-11-21T10:00:00Z", to = "2026-11-21T09:00:00Z").statusCode.value())
        assertEquals(422, controller.stats(request(), "2026-11-21T10:00:00Z", "2026-11-21T09:00:00Z", now).statusCode.value())
        assertTrue(received.isEmpty())
    }

    @Test
    fun `시각 꼴이 아니면 422`() {
        assertEquals(422, logs(from = "어제").statusCode.value())
    }

    @Test
    fun `q 가 1자면 422 이고 2자면 찾는다`() {
        assertEquals(422, logs(q = "a").statusCode.value())
        assertEquals(422, logs(q = " a ").statusCode.value(), "공백을 빼면 1자다")
        assertEquals(200, logs(q = "ab").statusCode.value())
        assertEquals("ab", received.single().text)
    }

    @Test
    fun `limit 은 500 까지이고 넘거나 숫자가 아니면 422`() {
        assertEquals(200, logs(limit = "500").statusCode.value())
        assertEquals(500, received.single().limit)
        assertEquals(422, logs(limit = "501").statusCode.value())
        assertEquals(422, logs(limit = "0").statusCode.value())
        assertEquals(422, logs(limit = "-1").statusCode.value())
        assertEquals(422, logs(limit = "many").statusCode.value())
    }

    @Test
    fun `커서가 응답의 꼴이 아니면 422`() {
        for (cursor in listOf("abc", "123", "1-2-3", "1763700000123-x")) assertEquals(422, logs(before = cursor).statusCode.value(), cursor)
        assertTrue(received.isEmpty())
        assertEquals(200, logs(before = "1763700000123-4").statusCode.value())
        assertEquals(StreamId(1_763_700_000_123, 4), received.single().before)
    }

    @Test
    fun `조건이 저장소에 그대로 넘어간다`() {
        logs(from = "2026-11-21T10:00:00Z", to = "2026-11-21T11:00:00Z", service = "backend", level = "error", requestId = "req-abc", q = "timeout")
        val query = received.single()
        assertEquals(LogQuery(Instant.parse("2026-11-21T10:00:00Z"), Instant.parse("2026-11-21T11:00:00Z"), "backend", "error", "req-abc", "timeout", null, query.limit), query)
    }

    @Test
    fun `꽉 찬 쪽이면 마지막 항목의 ID 를 다음 커서로 주고 덜 찼으면 비운다`() {
        answer = { query -> (0 until query.limit).map { LogEntry(StreamId(10_000L - it, 0), "info", "backend", "req-1", "m$it") } }
        assertEquals("9998-0", data(logs(limit = "3"))["next_before"])
        answer = { listOf(LogEntry(StreamId(5, 0), "info", "backend", "req-1", "m")) }
        assertEquals("", data(logs(limit = "3"))["next_before"])
    }

    @Test
    fun `헬스는 비밀값 없이 열린다`() {
        collector.runOnce()
        assertEquals(200, controller.health().statusCode.value())
    }
}
