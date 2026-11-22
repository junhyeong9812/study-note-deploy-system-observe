package xyz.junproject.observe.auth

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.http.ResponseEntity
import org.springframework.mock.web.MockHttpServletRequest
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 명세 ② 관측 서버 추가분·세부의 관측 서버 계약: 경로 넷 전부 `X-Observe-Secret`(설정이 비면 거절),
 * 봉투 `{success,data}` / `{success:false,error}`, 본문 꼴이 틀리면 422 이고 저장소에 닿지 않는다.
 */
class AuthAttemptControllerTest {
    private val now = Instant.parse("2026-11-22T03:00:00Z")
    private val attemptId = "0b9c1d0e-8a3f-4c55-9a51-2f7e1c3d4b6a"

    /** 저장소 Fake — 받은 것을 모으고 정해 둔 답을 준다 */
    private class FakeAuthAttempts : AuthAttempts {
        val decided = mutableListOf<AuthAttemptRequest>()
        val rejected = mutableListOf<AuthRejectedAttempt>()
        val queries = mutableListOf<AuthAttemptQuery>()
        val lockAsked = mutableListOf<Instant>()
        var verdict: (AuthAttemptRequest) -> AuthVerdict = { AuthVerdict(true, AuthOutcome.SUCCESS, null, false, 0) }
        var rows: (AuthAttemptQuery) -> List<AuthAttemptRow> = { emptyList() }
        var status: AuthLockStatus = AuthLockStatus(false, null, 0)
        var failure: RuntimeException? = null

        override fun createSchema() {}
        override fun decide(request: AuthAttemptRequest): AuthVerdict { failure?.let { throw it }; decided += request; return verdict(request) }
        override fun recordRejected(attempt: AuthRejectedAttempt) { failure?.let { throw it }; rejected += attempt }
        override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> { failure?.let { throw it }; queries += query; return rows(query) }
        override fun lockStatus(now: Instant): AuthLockStatus { failure?.let { throw it }; lockAsked += now; return status }
        override fun deleteOlderThan(cutoff: Instant): Int = 0
        val touched get() = decided.size + rejected.size + queries.size + lockAsked.size
    }

    private val attempts = FakeAuthAttempts()
    private val controller = AuthAttemptController(attempts, "the-secret")

    private fun request(secret: String? = "the-secret") = MockHttpServletRequest().apply { if (secret != null) addHeader("X-Observe-Secret", secret) }

    private fun attemptBody(vararg overrides: Pair<String, Any?>): Map<String, Any?> = linkedMapOf<String, Any?>(
        "attempt_id" to attemptId, "at" to "2026-11-22T02:59:59.500Z", "credential_ok" to false,
        "ip" to "203.0.113.7", "user_agent" to "Mozilla/5.0",
    ).apply { overrides.forEach { (key, value) -> if (value == REMOVE) remove(key) else put(key, value) } }

    private fun rejectedBody(vararg overrides: Pair<String, Any?>): Map<String, Any?> = linkedMapOf<String, Any?>(
        "attempt_id" to attemptId, "at" to "2026-11-22T02:59:59.500Z", "reason" to "rate_limited",
        "ip" to "203.0.113.7", "user_agent" to "Mozilla/5.0",
    ).apply { overrides.forEach { (key, value) -> if (value == REMOVE) remove(key) else put(key, value) } }

    private fun list(secret: String? = "the-secret", from: String? = null, to: String? = null, before: String? = null, limit: String? = null) =
        controller.list(request(secret), from, to, before, limit, now)

    @Suppress("UNCHECKED_CAST")
    private fun body(response: ResponseEntity<out Any>) = response.body as Map<String, Any?>
    @Suppress("UNCHECKED_CAST")
    private fun data(response: ResponseEntity<out Any>) = body(response)["data"] as Map<String, Any?>
    @Suppress("UNCHECKED_CAST")
    private fun errorCode(response: ResponseEntity<out Any>) = (body(response)["error"] as Map<String, Any?>)["code"]

    // ── 인증 ──────────────────────────────────────────────

    @Test
    fun `비밀값이 없거나 틀리거나 비면 경로 넷 모두 401 이고 저장소에 닿지 않는다`() {
        for (secret in listOf(null, "wrong", "", "the-secret ")) {
            val responses = listOf(
                controller.decide(request(secret), attemptBody()),
                controller.rejected(request(secret), rejectedBody()),
                list(secret = secret),
                controller.lock(request(secret), now),
            )
            for (response in responses) {
                assertEquals(401, response.statusCode.value(), "secret=$secret")
                assertEquals(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")), response.body)
            }
        }
        assertEquals(0, attempts.touched)
    }

    @Test
    fun `설정된 비밀값이 비어 있으면 빈 헤더로도 열리지 않는다`() {
        val unconfigured = AuthAttemptController(attempts, "")
        for (secret in listOf(null, "")) {
            assertEquals(401, unconfigured.decide(request(secret), attemptBody()).statusCode.value())
            assertEquals(401, unconfigured.rejected(request(secret), rejectedBody()).statusCode.value())
            assertEquals(401, unconfigured.list(request(secret), null, null, null, null, now).statusCode.value())
            assertEquals(401, unconfigured.lock(request(secret), now).statusCode.value())
        }
        assertEquals(0, attempts.touched)
    }

    @Test
    fun `인증보다 본문 검증이 먼저 오지 않는다 — 비밀값이 틀리면 본문이 틀려도 401`() {
        assertEquals(401, controller.decide(request("wrong"), mapOf("attempt_id" to "x")).statusCode.value())
        assertEquals(401, controller.decide(request(null), null).statusCode.value())
    }

    // ── POST /auth/attempts ───────────────────────────────

    @Test
    fun `판정 본문이 저장소에 정리된 값으로 넘어가고 replayed 는 기본 false`() {
        assertEquals(200, controller.decide(request(), attemptBody()).statusCode.value())
        assertEquals(
            AuthAttemptRequest(UUID.fromString(attemptId), Instant.parse("2026-11-22T02:59:59.500Z"), false, "203.0.113.7", "Mozilla/5.0", false),
            attempts.decided.single(),
        )
        // 세부 2: replayed:true 면 outcome 이 필수 — 이 사례는 그 계약에 맞춰 outcome 을 함께 보낸다
        controller.decide(request(), attemptBody("replayed" to true, "credential_ok" to true, "outcome" to "success"))
        assertTrue(attempts.decided.last().replayed)
        assertTrue(attempts.decided.last().credentialOk)
        assertNull(attempts.decided.first().outcome, "replayed:false 의 요청에 outcome 이 붙었다")
    }

    // ── 세부 2: 재전송의 결과(outcome) ─────────────────────

    @Test
    fun `replayed true 의 outcome 은 보낸 값 그대로 저장소에 넘어간다`() {
        val expected = mapOf("success" to AuthOutcome.SUCCESS, "failure" to AuthOutcome.FAILURE, "locked" to AuthOutcome.LOCKED)
        for ((wire, outcome) in expected) {
            val response = controller.decide(request(), attemptBody("replayed" to true, "credential_ok" to true, "outcome" to wire))
            assertEquals(200, response.statusCode.value(), "outcome=$wire ${response.body}")
            assertEquals(outcome, attempts.decided.last().outcome)
            assertTrue(attempts.decided.last().replayed)
        }
        assertEquals(3, attempts.decided.size)
    }

    @Test
    fun `replayed true 인데 outcome 이 없으면 422`() {
        for (value in listOf(null, REMOVE)) {
            val response = controller.decide(request(), attemptBody("replayed" to true, "outcome" to value))
            assertEquals(422, response.statusCode.value(), "outcome=$value")
            assertEquals("invalid_request", errorCode(response))
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `replayed true 의 outcome 은 success·failure·locked 만 받는다`() {
        for (value in listOf("rate_limited", "origin_rejected", "malformed", "SUCCESS", "Locked", "", " success", true, 1)) {
            val response = controller.decide(request(), attemptBody("replayed" to true, "outcome" to value))
            assertEquals(422, response.statusCode.value(), "outcome=$value")
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `replayed false 거나 빠졌는데 outcome 을 보내면 422`() {
        // 세부 3: 칸이 있으면 값이 null 이어도 422
        for (value in listOf("success", "failure", "locked", null)) {
            assertEquals(422, controller.decide(request(), attemptBody("replayed" to false, "outcome" to value)).statusCode.value(), "replayed=false outcome=$value")
            assertEquals(422, controller.decide(request(), attemptBody("outcome" to value)).statusCode.value(), "replayed 없음 outcome=$value")
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `판정 응답은 봉투 안에 다섯 칸이고 시각은 ISO 글자, 잠금이 없으면 locked_until 은 null`() {
        val until = Instant.parse("2026-11-22T03:15:00Z")
        attempts.verdict = { AuthVerdict(false, AuthOutcome.FAILURE, until, true, 5) }
        val locked = controller.decide(request(), attemptBody())
        assertEquals(true, body(locked)["success"])
        assertEquals(mapOf("allowed" to false, "outcome" to "failure", "locked_until" to "2026-11-22T03:15:00Z", "lock_started" to true, "failures" to 5), data(locked))

        attempts.verdict = { AuthVerdict(true, AuthOutcome.SUCCESS, null, false, 0) }
        val allowed = data(controller.decide(request(), attemptBody("credential_ok" to true)))
        assertEquals(mapOf("allowed" to true, "outcome" to "success", "locked_until" to null, "lock_started" to false, "failures" to 0), allowed)

        attempts.verdict = { AuthVerdict(false, AuthOutcome.LOCKED, until, false, 5) }
        assertEquals("locked", data(controller.decide(request(), attemptBody("credential_ok" to true)))["outcome"])
    }

    @Test
    fun `attempt_id 가 UUID 가 아니면 422`() {
        for (value in listOf("not-a-uuid", "", "1-1-1-1-1", "0b9c1d0e8a3f4c559a512f7e1c3d4b6a", 12345, null, REMOVE)) {
            val response = controller.decide(request(), attemptBody("attempt_id" to value))
            assertEquals(422, response.statusCode.value(), "attempt_id=$value")
            assertEquals("invalid_request", errorCode(response))
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `at 이 ISO 시각이 아니면 422`() {
        for (value in listOf("어제", "", "2026-11-22", "2026-11-22 03:00:00", 1763780400000L, null, REMOVE)) {
            assertEquals(422, controller.decide(request(), attemptBody("at" to value)).statusCode.value(), "at=$value")
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `credential_ok 와 replayed 가 참·거짓이 아니면 422`() {
        for (value in listOf("true", 1, null, REMOVE)) {
            assertEquals(422, controller.decide(request(), attemptBody("credential_ok" to value)).statusCode.value(), "credential_ok=$value")
        }
        for (value in listOf("true", 1)) {
            assertEquals(422, controller.decide(request(), attemptBody("replayed" to value)).statusCode.value(), "replayed=$value")
        }
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `replayed 칸이 있는데 null 이면 422 — 칸이 없을 때만 기본 false`() {
        // 세부 4: replayed 칸이 있으면 Boolean 이어야 한다
        assertEquals(422, controller.decide(request(), attemptBody("replayed" to null)).statusCode.value(), "replayed=null")
        assertEquals(422, controller.decide(request(), attemptBody("replayed" to null, "outcome" to "failure")).statusCode.value(), "replayed=null + outcome")
        assertTrue(attempts.decided.isEmpty())
        assertEquals(200, controller.decide(request(), attemptBody()).statusCode.value(), "replayed 칸 없음")
        assertFalse(attempts.decided.single().replayed)
    }

    @Test
    fun `거절 기록에 계약에 없는 칸이 와도 무시하고 기록한다`() {
        val extras = listOf<Pair<String, Any?>>("replayed" to true, "replayed" to null, "outcome" to "success", "credential_ok" to false, "unknown" to "x")
        for (extra in extras) {
            val response = controller.rejected(request(), rejectedBody(extra))
            assertEquals(200, response.statusCode.value(), "${extra.first}=${extra.second} ${response.body}")
            assertEquals("rate_limited", attempts.rejected.last().reason)
        }
        assertEquals(extras.size, attempts.rejected.size)
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `user_agent 는 200자까지 받고 넘으면 422`() {
        assertEquals(200, controller.decide(request(), attemptBody("user_agent" to "가".repeat(200))).statusCode.value())
        assertEquals("가".repeat(200), attempts.decided.single().userAgent)
        assertEquals(422, controller.decide(request(), attemptBody("user_agent" to "가".repeat(201))).statusCode.value())
        assertEquals(422, controller.decide(request(), attemptBody("user_agent" to 7)).statusCode.value())
        assertEquals(1, attempts.decided.size)
    }

    @Test
    fun `ip 가 없거나 비었거나 지나치게 길면 422`() {
        for (value in listOf("", "   ", "1".repeat(100), 7, null, REMOVE)) {
            assertEquals(422, controller.decide(request(), attemptBody("ip" to value)).statusCode.value(), "ip=$value")
        }
        assertEquals(200, controller.decide(request(), attemptBody("ip" to "2001:db8::1")).statusCode.value())
    }

    @Test
    fun `본문이 없으면 422`() {
        assertEquals(422, controller.decide(request(), null).statusCode.value())
        assertEquals(422, controller.rejected(request(), null).statusCode.value())
        assertEquals(0, attempts.touched)
    }

    @Test
    fun `판정 중 저장소 오류면 503 이고 예외 문구를 내지 않는다`() {
        attempts.failure = DataAccessResourceFailureException("Connection to postgres:5432 refused — 내부 문구")
        val response = controller.decide(request(), attemptBody())
        assertEquals(503, response.statusCode.value())
        assertFalse(response.body.toString().contains("내부 문구"))
        assertEquals(false, body(response)["success"])
    }

    // ── POST /auth/attempts/rejected ──────────────────────

    @Test
    fun `거절 기록은 저장소에 넘기고 판정을 부르지 않는다`() {
        val response = controller.rejected(request(), rejectedBody())
        assertEquals(200, response.statusCode.value())
        assertEquals(true, body(response)["success"])
        assertEquals(
            AuthRejectedAttempt(UUID.fromString(attemptId), Instant.parse("2026-11-22T02:59:59.500Z"), "rate_limited", "203.0.113.7", "Mozilla/5.0"),
            attempts.rejected.single(),
        )
        assertTrue(attempts.decided.isEmpty(), "거절 기록이 잠금 계산을 탔다")
    }

    @Test
    fun `거절 사유는 rate_limited·origin_rejected·malformed 를 받아 그대로 넘긴다`() {
        // 세부 2 기록 범위: 자격 검증 전에 거절한 것은 이 세 이유로 기록한다
        for (reason in listOf("rate_limited", "origin_rejected", "malformed")) {
            val response = controller.rejected(request(), rejectedBody("reason" to reason))
            assertEquals(200, response.statusCode.value(), "reason=$reason ${response.body}")
            assertEquals(reason, attempts.rejected.last().reason)
        }
        assertEquals(3, attempts.rejected.size)
        assertTrue(attempts.decided.isEmpty())
    }

    @Test
    fun `거절 사유는 세 낱말 밖이면 422`() {
        // not_configured 는 계약에서 철회됐다(어드민은 설정이 없으면 뜨지 않는다) — 세 낱말 밖이므로 422
        for (value in listOf("locked", "success", "failure", "", "RATE_LIMITED", "Origin_Rejected", "malformed ", "not_configured", 1, null, REMOVE)) {
            assertEquals(422, controller.rejected(request(), rejectedBody("reason" to value)).statusCode.value(), "reason=$value")
        }
        assertTrue(attempts.rejected.isEmpty())
    }

    @Test
    fun `거절 기록도 attempt_id·at·user_agent 꼴을 검사한다`() {
        assertEquals(422, controller.rejected(request(), rejectedBody("attempt_id" to "x")).statusCode.value())
        assertEquals(422, controller.rejected(request(), rejectedBody("at" to "어제")).statusCode.value())
        assertEquals(422, controller.rejected(request(), rejectedBody("user_agent" to "a".repeat(201))).statusCode.value())
        assertEquals(422, controller.rejected(request(), rejectedBody("ip" to "")).statusCode.value())
        assertTrue(attempts.rejected.isEmpty())
    }

    // ── GET /auth/attempts ────────────────────────────────

    private val from = "2026-11-21T00:00:00Z"
    private val to = "2026-11-22T00:00:00Z"

    @Test
    fun `limit 은 기본 50, 1~200 이고 넘거나 숫자가 아니면 422`() {
        assertEquals(200, list(from = from, to = to).statusCode.value())
        assertEquals(50, attempts.queries.single().limit)
        assertEquals(200, list(from = from, to = to, limit = "200").statusCode.value())
        assertEquals(200, attempts.queries.last().limit)
        assertEquals(200, list(from = from, to = to, limit = "1").statusCode.value())
        for (value in listOf("201", "0", "-1", "many")) assertEquals(422, list(from = from, to = to, limit = value).statusCode.value(), "limit=$value")
        assertEquals(3, attempts.queries.size)
    }

    @Test
    fun `기간은 최대 90일이고 넘거나 거꾸로거나 시각 꼴이 아니면 422`() {
        val end = Instant.parse("2026-11-22T00:00:00Z")
        assertEquals(200, list(from = end.minus(Duration.ofDays(90)).toString(), to = end.toString()).statusCode.value())
        assertEquals(422, list(from = end.minus(Duration.ofDays(90)).minusMillis(1).toString(), to = end.toString()).statusCode.value())
        assertEquals(422, list(from = to, to = from).statusCode.value())
        assertEquals(422, list(from = "어제", to = to).statusCode.value())
        assertEquals(1, attempts.queries.size)
    }

    @Test
    fun `기간을 주지 않아도 지금을 끝으로 90일 안의 기간을 찾는다`() {
        assertEquals(200, list().statusCode.value())
        val query = attempts.queries.single()
        assertEquals(now, query.to)
        assertFalse(query.from.isAfter(query.to))
        assertTrue(Duration.between(query.from, query.to) <= Duration.ofDays(90))
    }

    @Test
    fun `조건이 저장소에 그대로 넘어가고 before 는 응답의 next_before 를 되읽는다`() {
        val cursor = AuthAttemptCursor(Instant.parse("2026-11-21T12:00:00Z"), UUID.fromString(attemptId))
        assertEquals(200, list(from = from, to = to, before = cursor.toString(), limit = "7").statusCode.value())
        assertEquals(AuthAttemptQuery(Instant.parse(from), Instant.parse(to), cursor, 7), attempts.queries.single())
    }

    @Test
    fun `before 가 응답의 꼴이 아니면 422`() {
        for (value in listOf("abc", "123", attemptId)) assertEquals(422, list(from = from, to = to, before = value).statusCode.value(), "before=$value")
        assertTrue(attempts.queries.isEmpty())
    }

    @Test
    fun `before 만 넘기고 from·to 를 둘 다 빼면 422`() {
        // 쪽마다 기본 창을 다시 세면 창의 오래된 끝이 밀려 조용히 빠진다 — /logs 와 같은 규칙
        val cursor = AuthAttemptCursor(Instant.parse("2026-11-21T12:00:00Z"), UUID.fromString(attemptId))
        assertEquals(422, list(before = cursor.toString()).statusCode.value())
        assertTrue(attempts.queries.isEmpty())
    }

    @Test
    fun `목록 응답은 attempts 와 next_before — 꽉 찼으면 마지막 행의 기준, 덜 찼으면 빈 글자`() {
        val base = Instant.parse("2026-11-21T12:00:00Z")
        val ids = (0 until 3).map { UUID(0L, it.toLong()) }
        attempts.rows = { query -> ids.take(query.limit).mapIndexed { index, id ->
            AuthAttemptRow(id, base.minusSeconds(index.toLong()), if (index == 0) AuthOutcome.LOCKED else AuthOutcome.RATE_LIMITED, "203.0.113.7", "UA-$index", index == 2)
        } }
        val full = data(list(from = from, to = to, limit = "2"))
        @Suppress("UNCHECKED_CAST")
        val rows = full["attempts"] as List<Map<String, Any?>>
        assertEquals(
            mapOf("id" to ids[0].toString(), "at" to "2026-11-21T12:00:00Z", "outcome" to "locked", "ip" to "203.0.113.7", "user_agent" to "UA-0", "replayed" to false),
            rows[0],
        )
        assertEquals("rate_limited", rows[1]["outcome"])
        assertEquals(AuthAttemptCursor(base.minusSeconds(1), ids[1]).toString(), full["next_before"])

        val partial = data(list(from = from, to = to, limit = "5"))
        @Suppress("UNCHECKED_CAST")
        assertEquals(3, (partial["attempts"] as List<Any>).size)
        assertEquals(true, ((partial["attempts"] as List<*>)[2] as Map<*, *>)["replayed"])
        assertEquals("", partial["next_before"])
    }

    @Test
    fun `목록의 outcome 은 거절 이유를 구별해 보인다`() {
        val base = Instant.parse("2026-11-21T12:00:00Z")
        val outcomes = listOf(AuthOutcome.RATE_LIMITED, AuthOutcome.ORIGIN_REJECTED, AuthOutcome.MALFORMED)
        attempts.rows = { _ -> outcomes.mapIndexed { index, outcome -> AuthAttemptRow(UUID(0L, index.toLong()), base.minusSeconds(index.toLong()), outcome, "203.0.113.7", "UA", false) } }
        @Suppress("UNCHECKED_CAST")
        val rows = data(list(from = from, to = to))["attempts"] as List<Map<String, Any?>>
        assertEquals(listOf("rate_limited", "origin_rejected", "malformed"), rows.map { it["outcome"] })
    }

    @Test
    fun `목록 중 저장소 오류면 503`() {
        attempts.failure = DataAccessResourceFailureException("내부 문구")
        val response = list(from = from, to = to)
        assertEquals(503, response.statusCode.value())
        assertFalse(response.body.toString().contains("내부 문구"))
    }

    // ── GET /auth/lock ────────────────────────────────────

    @Test
    fun `잠금 상태는 지금 시각으로 묻고 봉투 안에 세 칸`() {
        attempts.status = AuthLockStatus(true, Instant.parse("2026-11-22T03:10:00Z"), 5)
        val response = controller.lock(request(), now)
        assertEquals(200, response.statusCode.value())
        assertEquals(true, body(response)["success"])
        assertEquals(mapOf("locked" to true, "locked_until" to "2026-11-22T03:10:00Z", "failures" to 5), data(response))
        assertEquals(listOf(now), attempts.lockAsked)

        attempts.status = AuthLockStatus(false, null, 2)
        assertEquals(mapOf("locked" to false, "locked_until" to null, "failures" to 2), data(controller.lock(request(), now)))
    }

    @Test
    fun `목록·잠금 상한 수치는 명세 그대로`() {
        assertEquals(200, AuthAttemptController.MAX_LIMIT)
        assertEquals(50, AuthAttemptController.DEFAULT_LIMIT)
        assertEquals(200, AuthAttemptController.MAX_USER_AGENT_LENGTH)
        assertEquals(Duration.ofDays(90), AuthAttemptController.MAX_RANGE)
    }

    private companion object {
        /** 본문에서 칸을 아예 빼라는 표시 */
        val REMOVE = Any()
    }
}
