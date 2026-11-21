package xyz.junproject.observe.api

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.UncategorizedSQLException
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
import java.sql.SQLException
import java.time.Duration
import java.time.Instant

/** 명세 ②: 조회는 `X-Observe-Secret` 없으면 401, 조건이 틀리면 422. 기간을 안 주면 최근 1시간, 최대 500건, 커서 이어 보기. */
class LogControllerTest {
    private val now = Instant.parse("2026-11-21T12:00:00Z")
    private val received = mutableListOf<LogQuery>()
    private var answer: (LogQuery) -> List<LogEntry> = { emptyList() }
    private var statsAnswer: (Instant, Instant) -> List<LogCount> = { from, _ -> listOf(LogCount(from, "backend", "error", 3)) }
    private val reader = object : LogReader {
        override fun search(query: LogQuery): List<LogEntry> { received += query; return answer(query) }
        override fun stats(from: Instant, to: Instant): List<LogCount> = statsAnswer(from, to)
    }
    private val stream = FakeLogStream().also { it.ensureGroup() }
    private val collector = LogCollector(stream, FakeLogSink(), block = Duration.ZERO)
    private val health = CollectorHealth(collector, { stream.unacknowledgedCount() }, { 0L }, { null }, { null })
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
        // 기간을 함께 넘겨 커서의 꼴만 보게 한다 — 기간 없이 커서만 넘기면 그것만으로 422 다(아래 테스트)
        val from = "2026-11-21T10:00:00Z"
        val to = "2026-11-21T11:00:00Z"
        for (cursor in listOf("abc", "123", "1-2-3", "1763700000123-x")) assertEquals(422, logs(before = cursor, from = from, to = to).statusCode.value(), cursor)
        assertTrue(received.isEmpty())
        assertEquals(200, logs(before = "1763700000123-4", from = from, to = to).statusCode.value())
        assertEquals(StreamId(1_763_700_000_123, 4), received.single().before)
    }

    @Test
    fun `커서만 넘기고 from·to 를 둘 다 빼면 422 이고 조회하지 않는다`() {
        // 쪽마다 "지금부터 1시간"을 다시 세면 창의 오래된 끝이 밀려 그 사이 로그가 조용히 빠진다 — 응답의 from·to 를 함께 받는다
        val response = logs(before = "1763700000123-4")
        assertEquals(422, response.statusCode.value())
        @Suppress("UNCHECKED_CAST")
        val error = (response.body as Map<String, Any>)["error"] as Map<String, Any>
        assertEquals("이어 보려면 응답의 from·to 를 함께 넘긴다", error["detail"])
        assertTrue(received.isEmpty())
        // 한쪽만 있어도 창의 오래된 끝이 고정되므로 받는다
        assertEquals(200, logs(before = "1763700000123-4", from = "2026-11-21T10:00:00Z").statusCode.value())
        assertEquals(200, logs(before = "1763700000123-4", to = "2026-11-21T11:00:00Z").statusCode.value())
    }

    private val failedQuery = mapOf("success" to false, "error" to mapOf("code" to "query_failed"))
    private val timedOutQuery = mapOf("success" to false, "error" to mapOf("code" to "query_timeout"))

    @Test
    fun `조회가 statement_timeout 으로 끊기면 504 query_timeout 이고 예외 문구를 내지 않는다`() {
        answer = { throw QueryTimeoutException("canceling statement due to statement timeout — 내부 문구") }
        val response = logs()
        assertEquals(504, response.statusCode.value())
        assertEquals(timedOutQuery, response.body)
    }

    @Test
    fun `SQLSTATE 57014 로 끊긴 조회도 504 query_timeout 이다`() {
        // 번역기가 QueryTimeoutException 이 아닌 다른 종류로 감싸도 원인의 SQLSTATE 로 알아본다
        answer = { throw UncategorizedSQLException("search", "SELECT 내부 문장", SQLException("canceling statement due to statement timeout", "57014")) }
        assertEquals(504, logs().statusCode.value())
        statsAnswer = { _, _ -> throw QueryTimeoutException("내부 문구") }
        val stats = controller.stats(request(), null, null, now)
        assertEquals(504, stats.statusCode.value())
        assertEquals(timedOutQuery, stats.body)
    }

    @Test
    fun `조회 중 DB 오류면 503 query_failed 이고 예외 문구를 내지 않는다`() {
        answer = { throw DataAccessResourceFailureException("Connection to postgres:5432 refused — 내부 문구") }
        val response = logs()
        assertEquals(503, response.statusCode.value())
        assertEquals(failedQuery, response.body)
        statsAnswer = { _, _ -> throw DataAccessResourceFailureException("내부 문구") }
        val stats = controller.stats(request(), null, null, now)
        assertEquals(503, stats.statusCode.value())
        assertEquals(failedQuery, stats.body)
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
