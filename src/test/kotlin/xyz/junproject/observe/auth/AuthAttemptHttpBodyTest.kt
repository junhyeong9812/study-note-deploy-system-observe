package xyz.junproject.observe.auth

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.http.ResponseEntity
import org.springframework.mock.web.MockHttpServletRequest
import java.time.Instant
import java.util.UUID

/**
 * 세부 3 — 두 POST 경로의 HTTP 입구(원문 본문을 이 컨트롤러가 해석한다): 비밀값이 먼저, 그다음 Content-Type · 크기 상한 · JSON 객체.
 * 실제 서버를 거친 같은 규칙은 엔진 사례(`AuthAttemptHttpEngineTest`·`ApplicationSmokeEngineTest`)가 본다.
 */
class AuthAttemptHttpBodyTest {
    private val recorded = mutableListOf<String>()
    private val attempts = object : AuthAttempts {
        override fun createSchema() {}
        override fun decide(request: AuthAttemptRequest): AuthVerdict { recorded += "decide"; return AuthVerdict(false, AuthOutcome.FAILURE, null, false, 1) }
        override fun recordRejected(attempt: AuthRejectedAttempt) { recorded += "rejected" }
        override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> = emptyList()
        override fun lockStatus(now: Instant): AuthLockStatus = AuthLockStatus(false, null, 0)
        override fun deleteOlderThan(cutoff: Instant): Int = 0
    }
    private val controller = AuthAttemptController(attempts, "the-secret")

    private fun post(body: ByteArray?, contentType: String? = "application/json", secret: String? = "the-secret") = MockHttpServletRequest("POST", "/auth/attempts").apply {
        if (secret != null) addHeader("X-Observe-Secret", secret)
        if (contentType != null) this.contentType = contentType
        if (body != null) setContent(body)
    }

    private fun attemptJson(userAgent: String = "Mozilla/5.0") =
        """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"$userAgent"}"""

    @Suppress("UNCHECKED_CAST")
    private fun errorCode(response: ResponseEntity<out Any>) = ((response.body as Map<String, Any?>)["error"] as Map<String, Any?>)["code"]

    @Test
    fun `정상 본문은 두 POST 입구를 지나 저장소에 닿는다 — charset 이 붙은 JSON 도`() {
        assertEquals(200, controller.decide(post(attemptJson().toByteArray())).statusCode.value())
        assertEquals(200, controller.decide(post(attemptJson().toByteArray(), "application/json; charset=utf-8")).statusCode.value())
        val rejected = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","reason":"malformed","ip":"203.0.113.7","user_agent":"curl/8"}"""
        assertEquals(200, controller.rejected(post(rejected.toByteArray())).statusCode.value())
        assertEquals(listOf("decide", "decide", "rejected"), recorded)
    }

    @Test
    fun `본문 상한을 넘으면 422 이고 저장소에 닿지 않는다 — 상한 안의 정상 본문은 지난다`() {
        val padding = " ".repeat(AuthAttemptController.MAX_BODY_BYTES - attemptJson().length)   // 공백으로 상한까지 채운 정상 JSON
        val atLimit = (attemptJson() + padding).toByteArray()
        assertEquals(AuthAttemptController.MAX_BODY_BYTES, atLimit.size)
        assertEquals(200, controller.decide(post(atLimit)).statusCode.value())

        val overLimit = (attemptJson() + padding + " ").toByteArray()
        for (response in listOf(controller.decide(post(overLimit)), controller.rejected(post(overLimit)))) {
            assertEquals(422, response.statusCode.value())
            assertEquals("invalid_request", errorCode(response))
        }
        val huge = ByteArray(5 * 1024 * 1024) { 'a'.code.toByte() }
        assertEquals(422, controller.decide(post(huge)).statusCode.value())
        assertEquals(listOf("decide"), recorded)
    }

    @Test
    fun `비밀값이 틀리면 본문 상한·Content-Type 보다 먼저 401`() {
        val huge = ByteArray(1024 * 1024)
        assertEquals(401, controller.decide(post(huge, contentType = "text/plain", secret = "wrong")).statusCode.value())
        assertEquals(401, controller.rejected(post(null, contentType = null, secret = null)).statusCode.value())
        assertTrue(recorded.isEmpty())
    }

    @Test
    fun `칸은 있는데 값이 null 이면 필수 칸이든 replayed 든 422 — 두 POST 경로 모두`() {
        // 세부 4: 칸이 있으면 그 칸의 꼴이어야 한다. 칸이 없을 때의 기본값은 replayed(false) 하나뿐
        val decideFields = listOf("attempt_id", "at", "credential_ok", "ip", "user_agent", "replayed")
        for (field in decideFields) {
            val body = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"Mozilla/5.0","replayed":false}"""
                .replace(Regex("\"$field\":(\"[^\"]*\"|false)"), "\"$field\":null")
            assertTrue(body.contains("\"$field\":null"), body)
            assertEquals(422, controller.decide(post(body.toByteArray())).statusCode.value(), "decide $field=null")
        }
        for (field in listOf("attempt_id", "at", "reason", "ip", "user_agent")) {
            val body = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","reason":"malformed","ip":"203.0.113.7","user_agent":"curl/8"}"""
                .replace(Regex("\"$field\":\"[^\"]*\""), "\"$field\":null")
            assertTrue(body.contains("\"$field\":null"), body)
            assertEquals(422, controller.rejected(post(body.toByteArray())).statusCode.value(), "rejected $field=null")
        }
        assertTrue(recorded.isEmpty())
    }

    @Test
    fun `깨진 Content-Type 머리글이나 JSON 뒤의 군더더기는 422`() {
        assertEquals(422, controller.decide(post(attemptJson().toByteArray(), contentType = "not a media type")).statusCode.value())
        assertEquals(422, controller.decide(post((attemptJson() + " {}").toByteArray())).statusCode.value())
        assertTrue(recorded.isEmpty())
    }
}
