package xyz.junproject.observe.visit

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.time.LocalDate

/**
 * `GET /visits/summary?days=` — 기존 조회(`/logs` · `/metrics/…`)와 같은 봉투: `X-Observe-Secret`(입력 검사보다 먼저, 설정이 비면 전부 거절) ·
 * 401 `unauthorized` · 422 `invalid_request`(`detail`) · DB 오류 503 `query_failed` · 시간 초과 504 `query_timeout`.
 * `days` 는 7 · 30 · 90 만(없으면 7). 응답은 요약 수치만 — 칸 이름은 어드민 화면의 계약이다.
 * 저장소는 Fake — 경계에서 넘기는 값과 응답의 꼴만 본다(실제 저장소를 거친 것은 엔진 테스트).
 */
class VisitControllerTest {
    private val now = Instant.parse("2026-11-25T03:00:00Z")

    private class FakeVisitReader : VisitReader {
        val asked = mutableListOf<Pair<Int, Instant>>()
        var failure: RuntimeException? = null
        var answer: (Int, Instant) -> VisitSummary = { days, now ->
            VisitSummary(days, now, now, emptyList(), emptyList(), false, emptyList(), false, VisitDevice.entries.associateWith { 0L })
        }

        override fun summary(days: Int, now: Instant): VisitSummary { failure?.let { throw it }; asked += days to now; return answer(days, now) }
    }

    private val reader = FakeVisitReader()
    private val controller = VisitController(reader, "the-secret")

    private fun request(secret: String? = "the-secret") = MockHttpServletRequest("GET", "/visits/summary").apply { if (secret != null) addHeader("X-Observe-Secret", secret) }

    private fun summary(days: String?, secret: String? = "the-secret", target: VisitController = controller) = target.summary(request(secret), days, now)

    @Suppress("UNCHECKED_CAST")
    private fun data(response: ResponseEntity<out Any>): Map<String, Any?> {
        assertEquals(200, response.statusCode.value(), "${response.body}")
        val body = response.body as Map<String, Any?>
        assertEquals(true, body["success"])
        return body["data"] as Map<String, Any?>
    }

    private val unauthorized = mapOf("success" to false, "error" to mapOf("code" to "unauthorized"))

    @Test
    fun `비밀값이 없거나 틀리면 401 이고 days 가 틀려도 401 이 먼저이며 저장소에 닿지 않는다`() {
        for (secret in listOf(null, "wrong", "")) {
            for (days in listOf("7", "1", null)) {
                val response = summary(days, secret)
                assertEquals(401, response.statusCode.value(), "secret=$secret days=$days")
                assertEquals(unauthorized, response.body)
            }
        }
        assertTrue(reader.asked.isEmpty())
    }

    @Test
    fun `설정의 비밀값이 비어 있으면 빈 머리글도 거절한다`() {
        val open = VisitController(reader, "")
        assertEquals(401, summary("7", secret = "", target = open).statusCode.value())
    }

    @Test
    fun `days 는 7 · 30 · 90 만 받고 없으면 7 이다`() {
        for (days in listOf("7", "30", "90")) assertEquals(200, summary(days).statusCode.value(), days)
        summary(null)
        assertEquals(listOf(7 to now, 30 to now, 90 to now, 7 to now), reader.asked)
    }

    @Test
    fun `days 가 허용 목록 밖이면 422 invalid_request 이고 저장소에 닿지 않는다`() {
        for (days in listOf("1", "6", "8", "31", "91", "365", "0", "-7", "seven", "7.0", " 7")) {
            val response = summary(days)
            assertEquals(422, response.statusCode.value(), "days=$days ${response.body}")
            @Suppress("UNCHECKED_CAST")
            val error = (response.body as Map<String, Any?>)["error"] as Map<String, Any?>
            assertEquals("invalid_request", error["code"], days)
            assertTrue((error["detail"] as? String)?.isNotBlank() == true, "detail 이 없다")
        }
        assertTrue(reader.asked.isEmpty(), "${reader.asked}")
    }

    @Test
    fun `요약의 칸 이름과 값 — 직접 들어온 곳은 host null 이고 봇 비율은 봇 수 나누기 전체 수다`() {
        reader.answer = { days, now ->
            VisitSummary(
                days = days, from = Instant.parse("2026-11-18T15:00:00Z"), to = now,
                daily = listOf(VisitDay(LocalDate.parse("2026-11-24"), 0, 0, 0, 0), VisitDay(LocalDate.parse("2026-11-25"), 3, 4, 2, 3)),
                topPaths = listOf(PathViews("/wiki/a", 2), PathViews("/", 1)), topPathsTruncated = true,
                topReferrers = listOf(ReferrerViews(null, 2), ReferrerViews("www.google.com", 1)), topReferrersTruncated = false,
                devices = mapOf(VisitDevice.DESKTOP to 2L, VisitDevice.MOBILE to 1L, VisitDevice.BOT to 1L),
            )
        }
        val data = data(summary("7"))
        assertEquals(mapOf(
            "days" to 7,
            "timezone" to "Asia/Seoul",
            "from" to "2026-11-18T15:00:00Z",
            "to" to now.toString(),
            "daily" to listOf(
                mapOf("date" to "2026-11-24", "views" to 0L, "views_with_bots" to 0L, "visitors" to 0L, "visitors_with_bots" to 0L),
                mapOf("date" to "2026-11-25", "views" to 3L, "views_with_bots" to 4L, "visitors" to 2L, "visitors_with_bots" to 3L),
            ),
            "top_paths" to mapOf("items" to listOf(mapOf("path" to "/wiki/a", "views" to 2L), mapOf("path" to "/", "views" to 1L)), "truncated" to true),
            "top_referrers" to mapOf("items" to listOf(mapOf("host" to null, "views" to 2L), mapOf("host" to "www.google.com", "views" to 1L)), "truncated" to false),
            "devices" to mapOf("desktop" to 2L, "mobile" to 1L, "bot" to 1L),
            "bot_ratio" to 0.25,
        ), data)
    }

    @Test
    fun `방문이 하나도 없으면 봇 비율은 0 이다`() {
        assertEquals(0.0, data(summary("30"))["bot_ratio"])
    }

    @Test
    fun `DB 오류는 503 query_failed 이고 시간 초과는 504 query_timeout 이며 예외 문구는 응답에 없다`() {
        reader.failure = DataAccessResourceFailureException("Connection refused jdbc:postgresql://postgres:5432")
        val failed = summary("7")
        assertEquals(503, failed.statusCode.value())
        assertEquals(mapOf("success" to false, "error" to mapOf("code" to "query_failed")), failed.body)
        reader.failure = QueryTimeoutException("canceling statement due to statement timeout")
        val timedOut = summary("7")
        assertEquals(504, timedOut.statusCode.value())
        assertEquals(mapOf("success" to false, "error" to mapOf("code" to "query_timeout")), timedOut.body)
    }

    @Test
    fun `경로는 GET visits summary 이고 컨트롤러는 스프링이 찾는 빈이다`() {
        assertNotNull(VisitController::class.java.getAnnotation(RestController::class.java), "@RestController 가 없다")
        val method = VisitController::class.java.methods.single { it.name == "summary" && it.getAnnotation(GetMapping::class.java) != null }
        assertEquals(listOf("/visits/summary"), method.getAnnotation(GetMapping::class.java).value.toList() + method.getAnnotation(GetMapping::class.java).path.toList())
    }
}
