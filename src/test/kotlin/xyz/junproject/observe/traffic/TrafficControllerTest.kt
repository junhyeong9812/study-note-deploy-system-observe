package xyz.junproject.observe.traffic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.mock.web.MockHttpServletRequest
import java.time.Duration
import java.time.Instant

/**
 * 규약 4.2 · 4.5 의 조회 — 경로 셋(`/traffic/map` · `/traffic/series` · `/traffic/requests`)의 인증(입력 검사보다 먼저 · 빈 설정이면 전부 거절),
 * 질의 검증, 답의 꼴, "채우는 중" 칸이 판정 수치에서 빠지고 따로 드러나는 것, 서버마다의 읽기 진도 · 수신 지연 · 순번의 틈, 상한과 절단.
 * 저장소는 가짜 — 저장소를 거친 것은 엔진 테스트.
 */
class TrafficControllerTest {
    private val now = Instant.parse("2026-11-25T04:10:00Z")

    private class FakeTrafficReader : TrafficReader {
        val cellQueries = mutableListOf<TrafficCellQuery>()
        val spanQueries = mutableListOf<Pair<String, Int>>()
        var cellRows: (TrafficCellQuery) -> List<TrafficCell> = { emptyList() }
        var progress: List<TrafficServerProgress> = emptyList()
        var summaries: Map<String, TrafficEntrySummary> = emptyMap()
        var spans: (Int) -> List<TrafficSpan> = { emptyList() }
        var failure: RuntimeException? = null
        val touched get() = cellQueries.size + spanQueries.size

        override fun cells(query: TrafficCellQuery): List<TrafficCell> { failure?.let { throw it }; cellQueries += query; return cellRows(query) }
        override fun serverProgress(): List<TrafficServerProgress> { failure?.let { throw it }; return progress }
        override fun entrySummaries(from: Instant, to: Instant): Map<String, TrafficEntrySummary> = summaries
        override fun requestSpans(requestId: String, limit: Int): List<TrafficSpan> { failure?.let { throw it }; spanQueries += requestId to limit; return spans(limit) }
    }

    private val reader = FakeTrafficReader()
    private val controller = TrafficController(reader, "the-secret")

    private fun request(secret: String? = "the-secret") = MockHttpServletRequest("GET", "/traffic").apply { if (secret != null) addHeader("X-Observe-Secret", secret) }

    @Suppress("UNCHECKED_CAST")
    private fun data(response: ResponseEntity<out Any>): Map<String, Any?> {
        assertEquals(200, response.statusCode.value(), "${response.body}")
        val body = response.body as Map<String, Any?>
        assertEquals(true, body["success"])
        return body["data"] as Map<String, Any?>
    }

    @Suppress("UNCHECKED_CAST")
    private fun errorCode(response: ResponseEntity<out Any>) = ((response.body as Map<String, Any?>)["error"] as Map<String, Any?>)["code"]

    private fun buckets(vararg pairs: Pair<Int, Long>) = LongArray(20).also { array -> pairs.forEach { (index, value) -> array[index] = value } }.toList()

    private fun cell(server: String, start: Instant, count: Long, status5xx: Long = 0, buckets: List<Long> = buckets(3 to count), maxMs: Long? = 40,
                     service: String = "backend", caller: String = "front", trafficClass: TrafficClass = TrafficClass.REQUEST, width: Duration = Duration.ofSeconds(30)) =
        TrafficCell(server, start, width, service, caller, trafficClass, TrafficTotals(count, TrafficStatusCounts(count - status5xx, 0, 0, status5xx, 0), buckets, maxMs))

    @Test
    fun `세 경로는 비밀값이 없거나 틀리거나 설정이 비면 401 이고 입력 검사 · 저장소보다 먼저다`() {
        val blank = TrafficController(reader, "")
        val calls: List<Pair<String, (TrafficController, String?) -> ResponseEntity<out Any>>> = listOf(
            "map" to { target, secret -> target.map(request(secret), "nope", null, now) },
            "series" to { target, secret -> target.series(request(secret), null, null, "visit", "nope", null, now) },
            "requests" to { target, secret -> target.requests(request(secret), "bad id!", now) },
        )
        for ((label, call) in calls) {
            for ((target, secret) in listOf(controller to null, controller to "wrong", blank to "", blank to "the-secret")) {
                val response = call(target, secret)
                assertEquals(401, response.statusCode.value(), "$label secret=$secret")
                assertEquals("unauthorized", errorCode(response), label)
            }
        }
        assertEquals(0, reader.touched, "인증 전에 저장소에 닿았다")
    }

    @Test
    fun `질의가 틀리면 422 invalid_request 이고 저장소에 닿지 않는다`() {
        val cases = listOf<Pair<String, () -> ResponseEntity<out Any>>>(
            "map from 이 ISO 가 아님" to { controller.map(request(), "yesterday", null, now) },
            "map 거꾸로" to { controller.map(request(), now.toString(), now.minusSeconds(1).toString(), now) },
            "map 24시간 초과" to { controller.map(request(), now.minus(Duration.ofHours(24)).minusMillis(1).toString(), now.toString(), now) },
            "series service 없음" to { controller.series(request(), null, null, null, null, null, now) },
            "series service 꼴" to { controller.series(request(), "Back End", null, null, null, null, now) },
            "series caller 꼴" to { controller.series(request(), "backend", "FRONT!", null, null, null, now) },
            "series 모르는 갈래" to { controller.series(request(), "backend", null, "visit", null, null, now) },
            "series 31일 초과" to { controller.series(request(), "backend", null, null, now.minus(Duration.ofDays(31)).minusMillis(1).toString(), now.toString(), now) },
            "requests 요청 ID 없음" to { controller.requests(request(), null, now) },
            "requests 요청 ID 꼴" to { controller.requests(request(), "req:1", now) },
            "requests 요청 ID 81자" to { controller.requests(request(), "a".repeat(81), now) },
        )
        for ((label, call) in cases) {
            val response = call()
            assertEquals(422, response.statusCode.value(), "$label: ${response.body}")
            assertEquals("invalid_request", errorCode(response), label)
        }
        assertEquals(0, reader.touched)
        // 경계의 안쪽은 받는다
        assertEquals(200, controller.map(request(), now.minus(Duration.ofHours(24)).toString(), now.toString(), now).statusCode.value())
        assertEquals(200, controller.series(request(), "backend", null, null, now.minus(Duration.ofDays(31)).toString(), now.toString(), now).statusCode.value())
        assertEquals(200, controller.requests(request(), "a".repeat(80), now).statusCode.value())
    }

    @Test
    fun `지도는 기간을 주지 않으면 최근 5분의 원본 칸을 묻고 선마다 수 · 오류율 · 중앙값 · 95% 를 채워진 칸에서만 낸다`() {
        val progress = Instant.parse("2026-11-25T04:09:00Z")
        reader.progress = listOf(TrafficServerProgress("ci-agent-158", progress, now.minusSeconds(20), 1, now.minusSeconds(20), firstSeqSeen = 5))
        reader.summaries = mapOf("ci-agent-158" to TrafficEntrySummary(listOf("backend-nginx:rotated"), 7, 3))
        val settled = Instant.parse("2026-11-25T04:08:00Z")             // 04:08:30 에 끝난다 — 진도 − 30초 = 04:08:30, 같으면 채워진 칸
        val filling = Instant.parse("2026-11-25T04:08:30Z")             // 04:09:00 에 끝난다 — 채우는 중
        reader.cellRows = { listOf(
            cell("ci-agent-158", settled, count = 20, status5xx = 2, buckets = buckets(0 to 10L, 5 to 8L, 7 to 1L, 19 to 1L), maxMs = 200_000),
            cell("ci-agent-158", filling, count = 50, status5xx = 50, buckets = buckets(19 to 50L), maxMs = 900_000),
            cell("ci-agent-158", settled, count = 4, service = "observe", caller = "alert", trafficClass = TrafficClass.HEALTH, buckets = List(20) { 0L }, maxMs = null),
        ) }
        val data = data(controller.map(request(), null, null, now))
        assertEquals(listOf(TrafficCellQuery(now.minus(Duration.ofMinutes(5)), now, now.minus(Duration.ofMinutes(5)))), reader.cellQueries)
        assertEquals(now.minus(Duration.ofMinutes(5)).toString(), data["from"])
        assertEquals(now.toString(), data["to"])
        assertEquals(
            listOf(mapOf("server" to "ci-agent-158", "read_progress_at" to progress.toString(), "receive_delayed" to false, "sequence_gaps" to 1L,
                "last_received_at" to now.minusSeconds(20).toString(), "missing" to listOf("backend-nginx:rotated"), "requests_dropped" to 7L,
                "requests_rejected" to 3L, "first_seq_seen" to 5L)),
            data["servers"],
        )
        assertEquals(
            listOf(
                mapOf("service" to "backend", "caller" to "front", "class" to "request", "count" to 20L,
                    "status" to mapOf("2xx" to 18L, "3xx" to 0L, "4xx" to 0L, "5xx" to 2L, "other" to 0L), "error_rate" to 0.1,
                    "p50" to mapOf("le_ms" to 5L), "p95" to mapOf("le_ms" to 1000L), "max_ms" to 200_000L, "filling_count" to 50L),
                mapOf("service" to "observe", "caller" to "alert", "class" to "health", "count" to 4L,
                    "status" to mapOf("2xx" to 4L, "3xx" to 0L, "4xx" to 0L, "5xx" to 0L, "other" to 0L), "error_rate" to 0.0,
                    "p50" to null, "p95" to null, "max_ms" to null, "filling_count" to 0L),
            ),
            data["edges"],
        )
    }

    @Test
    fun `읽기 진도가 없는 서버의 칸은 모두 채우는 중이고 진도가 2분 넘게 멈춘 서버는 수신 지연이다`() {
        reader.progress = listOf(
            TrafficServerProgress("ci-agent-158", null, null, 0, now.minusSeconds(5)),
            TrafficServerProgress("ci-agent-164", now.minus(Duration.ofMinutes(5)), now.minus(Duration.ofMinutes(2)).minusMillis(1), 3, now.minusSeconds(5)),
            TrafficServerProgress("ci-agent-170", now.minus(Duration.ofMinutes(5)), now.minus(Duration.ofMinutes(2)), 0, now.minusSeconds(5)),
        )
        reader.cellRows = { listOf(cell("ci-agent-158", now.minus(Duration.ofMinutes(4)), count = 9)) }
        val data = data(controller.map(request(), null, null, now))
        @Suppress("UNCHECKED_CAST")
        val servers = (data["servers"] as List<Map<String, Any?>>).associateBy { it["server"] }
        assertEquals(true, servers.getValue("ci-agent-158")["receive_delayed"])
        assertNull(servers.getValue("ci-agent-158")["read_progress_at"])
        assertEquals(true, servers.getValue("ci-agent-164")["receive_delayed"])
        assertEquals(3L, servers.getValue("ci-agent-164")["sequence_gaps"])
        assertEquals(false, servers.getValue("ci-agent-170")["receive_delayed"])
        @Suppress("UNCHECKED_CAST")
        val edge = (data["edges"] as List<Map<String, Any?>>).single()
        assertEquals(0L, edge["count"], "읽기 진도가 없는 서버의 칸을 판정 수치에 넣었다")
        assertEquals(9L, edge["filling_count"])
        assertNull(edge["error_rate"])
    }

    @Test
    fun `시계열은 간격마다 합친 수치와 채우는 중 표시를 내고 갈래의 기본은 request 다`() {
        val progress = Instant.parse("2026-11-25T04:09:00Z")
        reader.progress = listOf(TrafficServerProgress("ci-agent-158", progress, now.minusSeconds(20), 0, now.minusSeconds(20)))
        val from = Instant.parse("2026-11-25T03:10:00Z")
        reader.cellRows = { listOf(
            cell("ci-agent-158", Instant.parse("2026-11-25T03:10:00Z"), count = 3, status5xx = 1),
            cell("ci-agent-164", Instant.parse("2026-11-25T03:10:00Z"), count = 1),
            cell("ci-agent-158", Instant.parse("2026-11-25T04:08:30Z"), count = 2),
        ) }
        val data = data(controller.series(request(), "backend", null, null, null, null, now))
        val query = reader.cellQueries.single()
        assertEquals(TrafficCellQuery(from, now, query.rawSince, "backend", null, TrafficClass.REQUEST), query)
        assertEquals(mapOf("service" to "backend", "caller" to null, "class" to "request", "from" to from.toString(), "to" to now.toString(),
            "step_ms" to 30_000L, "source" to "raw"), data.filterKeys { it in setOf("service", "caller", "class", "from", "to", "step_ms", "source") })
        @Suppress("UNCHECKED_CAST")
        val points = data["points"] as List<Map<String, Any?>>
        assertEquals(listOf("2026-11-25T03:10:00Z", "2026-11-25T04:08:30Z"), points.map { it["at"] })
        assertEquals(4L, points[0]["count"])
        assertEquals(0.25, points[0]["error_rate"])
        assertEquals(true, points[0]["filling"], "진도가 없는 서버(ci-agent-164)의 칸이 섞인 점을 채워진 것으로 보였다")
        assertEquals(true, points[1]["filling"])
        assertNotNull(data["servers"])
    }

    @Test
    fun `요청 구간은 상한 500 개까지 싣고 넘치면 truncated 로 드러낸다`() {
        val line = TrafficRequest("req-1", "backend", "front", "GET", "/api/doc", false, 200, now.minusSeconds(3), 41, null, "200", "blue", 5120)
        reader.spans = { limit -> List(limit) { TrafficSpan("ci-agent-158", line) } }
        val data = data(controller.requests(request(), "req-1", now))
        assertEquals(listOf("req-1" to 501), reader.spanQueries, "넘쳤는지 보려면 하나 더 물어야 한다")
        @Suppress("UNCHECKED_CAST")
        val spans = data["spans"] as List<Map<String, Any?>>
        assertEquals(500, spans.size)
        assertEquals(true, data["truncated"])
        assertEquals(
            mapOf("server" to "ci-agent-158", "service" to "backend", "caller" to "front", "method" to "GET", "path" to "/api/doc", "path_truncated" to false,
                "status" to 200, "end_at" to now.minusSeconds(3).toString(), "duration_ms" to 41L, "upstream_ms" to null, "upstream_status" to "200",
                "slot" to "blue", "bytes_sent" to 5120L),
            spans.first(),
        )
        reader.spans = { listOf(TrafficSpan("ci-agent-158", line)) }
        assertEquals(false, data(controller.requests(request(), "req-1", now))["truncated"])
    }

    @Test
    fun `DB 오류는 503 query_failed 이고 시간 초과는 504 query_timeout 이다`() {
        reader.failure = DataAccessResourceFailureException("connection refused")
        assertEquals(503, controller.map(request(), null, null, now).statusCode.value())
        assertEquals("query_failed", errorCode(controller.requests(request(), "req-1", now)))
        reader.failure = QueryTimeoutException("canceling statement due to statement timeout")
        val timedOut = controller.series(request(), "backend", null, null, null, null, now)
        assertEquals(504, timedOut.statusCode.value())
        assertEquals("query_timeout", errorCode(timedOut))
    }
}
