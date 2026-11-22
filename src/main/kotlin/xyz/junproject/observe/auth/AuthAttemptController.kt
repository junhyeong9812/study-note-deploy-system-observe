package xyz.junproject.observe.auth

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.transaction.TransactionException
import org.springframework.web.bind.annotation.RestController
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID

/**
 * 로그인 시도 경로 — 전부 `X-Observe-Secret`(설정이 비면 전부 거절). 봉투는 `{success,data}` / `{success:false,error}`.
 * 본문은 맵으로 받아 꼴이 틀리면 422 `invalid_request`. 저장소 오류는 503 `query_failed`(예외 문구는 응답에 넣지 않는다).
 */
@RestController
class AuthAttemptController(
    private val attempts: AuthAttempts,
    @Value("\${observe.secret:}") private val secret: String,
) {
    private val log = LoggerFactory.getLogger("observe")

    private fun authorized(request: HttpServletRequest): Boolean {
        val provided = request.getHeader("X-Observe-Secret") ?: return false
        // 설정이 비어 있으면 항상 거절(fail-closed), 비교는 상수 시간 — /logs 와 같은 규칙
        return secret.isNotBlank() && MessageDigest.isEqual(provided.toByteArray(), secret.toByteArray())
    }

    private fun unauthorized() = ResponseEntity.status(401).body(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")))
    private fun invalid(detail: String) =
        ResponseEntity.status(422).body(mapOf("success" to false, "error" to mapOf("code" to "invalid_request", "detail" to detail)))
    private fun ok(data: Any) = ResponseEntity.ok(mapOf("success" to true, "data" to data))

    private class Invalid(val detail: String) : RuntimeException(detail)

    /**
     * 저장소 오류를 503 `query_failed` 로 — 연결을 못 얻어 트랜잭션을 시작하지 못한 것(TransactionException)도 같다.
     * 예외 문구는 서버 로그에만(앱 로그에는 IP·User-Agent 를 넣지 않는다 — SQL 의 값은 자리표시자라 문구에 나오지 않는다).
     */
    private fun answering(what: String, block: () -> ResponseEntity<out Any>): ResponseEntity<out Any> = try {
        block()
    } catch (error: Exception) {
        if (error !is DataAccessException && error !is TransactionException) throw error
        log.error("observe:observe:auth {} failed: {}: {}", what, error.javaClass.simpleName, error.message?.take(300))
        ResponseEntity.status(503).body(mapOf("success" to false, "error" to mapOf("code" to "query_failed")))
    }

    // ── 본문 칸 읽기 — 꼴이 틀리면 [Invalid] ──

    private fun Map<String, Any?>.uuid(name: String): UUID =
        (this[name] as? String)?.let(::parseUuid) ?: throw Invalid("$name 은 UUID 여야 한다")

    private fun Map<String, Any?>.instant(name: String): Instant =
        (this[name] as? String)?.let { parseInstant(name, it) } ?: throw Invalid("$name 은 ISO 시각이어야 한다(예: 2026-11-22T03:00:00Z)")

    private fun Map<String, Any?>.boolean(name: String): Boolean = this[name] as? Boolean ?: throw Invalid("$name 은 true·false 여야 한다")

    private fun Map<String, Any?>.ip(): String {
        val ip = this["ip"] as? String ?: throw Invalid("ip 는 글자여야 한다")
        if (ip.isBlank() || ip.length > MAX_IP_LENGTH) throw Invalid("ip 는 1~$MAX_IP_LENGTH 자")
        return ip
    }

    private fun Map<String, Any?>.userAgent(): String {
        val userAgent = this["user_agent"] as? String ?: throw Invalid("user_agent 는 글자여야 한다")
        if (userAgent.length > MAX_USER_AGENT_LENGTH) throw Invalid("user_agent 는 ${MAX_USER_AGENT_LENGTH}자 이하")
        return userAgent
    }

    private fun parseInstant(name: String, text: String): Instant =
        try { Instant.parse(text) } catch (_: DateTimeParseException) { throw Invalid("$name 은 ISO 시각이어야 한다(예: 2026-11-22T03:00:00Z)") }

    /**
     * 세부 2 — `outcome` 은 `replayed:true` 일 때만, 그리고 반드시(어드민이 임시 제한으로 실제 내린 판정). 글자는 정확히 같아야 한다.
     * `replayed:false`(또는 빠짐)인데 보내면 422 — 지금 판정을 받는 요청이 결과를 정해 보내는 것은 계약 위반이다.
     */
    private fun replayedOutcome(body: Map<String, Any?>, replayed: Boolean): AuthOutcome? {
        val value = body["outcome"]
        if (!replayed) {
            if (value != null) throw Invalid("outcome 은 replayed:true 일 때만 보낸다")
            return null
        }
        return REPLAYED_OUTCOMES.firstOrNull { it.wireName == value }
            ?: throw Invalid("replayed:true 면 outcome 은 ${REPLAYED_OUTCOMES.joinToString("·") { it.wireName }} 중 하나(필수)")
    }

    /** 판정 — `{attempt_id, at, credential_ok, ip, user_agent, replayed?, outcome?}` → `{allowed, outcome, locked_until, lock_started, failures}` */
    @PostMapping("/auth/attempts")
    fun decide(request: HttpServletRequest, @RequestBody(required = false) body: Map<String, Any?>?): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val attempt = try {
            if (body == null) throw Invalid("본문이 없다")
            val replayed = if (body["replayed"] == null) false else body.boolean("replayed")
            AuthAttemptRequest(
                attemptId = body.uuid("attempt_id"),
                at = body.instant("at"),
                credentialOk = body.boolean("credential_ok"),
                ip = body.ip(),
                userAgent = body.userAgent(),
                replayed = replayed,
                outcome = replayedOutcome(body, replayed),
            )
        } catch (error: Invalid) { return invalid(error.detail) }
        return answering("decide") {
            val verdict = attempts.decide(attempt)
            ok(mapOf(
                "allowed" to verdict.allowed, "outcome" to verdict.outcome.wireName, "locked_until" to verdict.lockedUntil?.toString(),
                "lock_started" to verdict.lockStarted, "failures" to verdict.failures,
            ))
        }
    }

    /** 자격 검증 전에 거절한 시도의 기록 — `{attempt_id, at, reason:"rate_limited"|"origin_rejected"|"malformed", ip, user_agent}` */
    @PostMapping("/auth/attempts/rejected")
    fun rejected(request: HttpServletRequest, @RequestBody(required = false) body: Map<String, Any?>?): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val attempt = try {
            if (body == null) throw Invalid("본문이 없다")
            val reason = REJECTED_OUTCOMES.firstOrNull { it.wireName == body["reason"] }
                ?: throw Invalid("reason 은 ${REJECTED_OUTCOMES.joinToString("·") { it.wireName }} 중 하나")
            AuthRejectedAttempt(body.uuid("attempt_id"), body.instant("at"), reason.wireName, body.ip(), body.userAgent())
        } catch (error: Invalid) { return invalid(error.detail) }
        return answering("record rejected") {
            attempts.recordRejected(attempt)
            ok(mapOf("recorded" to true))
        }
    }

    /** 목록 — 최신순, `{attempts:[{id,at,outcome,ip,user_agent,replayed}], next_before}` */
    @GetMapping("/auth/attempts")
    fun list(
        request: HttpServletRequest,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
        @RequestParam(required = false) before: String?,
        @RequestParam(required = false) limit: String?,
    ): ResponseEntity<out Any> = list(request, from, to, before, limit, Instant.now())

    /**
     * 기간: 둘 다 없으면 지금까지 [MAX_RANGE](보관 기간 전체), from 만 있으면 지금까지, to 만 있으면 그 앞 [MAX_RANGE].
     * 이어 보기는 `/logs` 와 같은 규칙 — 응답의 `next_before` 를 `before` 로, from·to 는 처음 것을 그대로 함께 넘긴다.
     */
    internal fun list(request: HttpServletRequest, from: String?, to: String?, before: String?, limit: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val query = try {
            if (!before.isNullOrBlank() && from.isNullOrBlank() && to.isNullOrBlank()) throw Invalid("이어 보려면 처음의 from·to 를 함께 넘긴다")
            val end = to?.takeIf { it.isNotBlank() }?.let { parseInstant("to", it) } ?: now
            val start = from?.takeIf { it.isNotBlank() }?.let { parseInstant("from", it) } ?: end.minus(MAX_RANGE)
            if (start.isAfter(end)) throw Invalid("from 이 to 보다 늦다")
            if (Duration.between(start, end) > MAX_RANGE) throw Invalid("기간은 최대 ${MAX_RANGE.toDays()}일")
            AuthAttemptQuery(
                from = start, to = end,
                before = before?.takeIf { it.isNotBlank() }?.let { AuthAttemptCursor.parse(it) ?: throw Invalid("before 는 응답의 next_before 값이어야 한다") },
                limit = (limit?.takeIf { it.isNotBlank() } ?: DEFAULT_LIMIT.toString()).toIntOrNull()?.takeIf { it in 1..MAX_LIMIT }
                    ?: throw Invalid("limit 은 1~$MAX_LIMIT"),
            )
        } catch (error: Invalid) { return invalid(error.detail) }
        return answering("list") {
            val rows = attempts.list(query)
            ok(mapOf(
                "attempts" to rows.map {
                    mapOf("id" to it.id.toString(), "at" to it.at.toString(), "outcome" to it.outcome.wireName, "ip" to it.ip,
                        "user_agent" to it.userAgent, "replayed" to it.replayed)
                },
                // 꽉 채워 받았으면 더 있을 수 있다 — 마지막 행을 다음 쪽의 기준으로
                "next_before" to (if (rows.size == query.limit) AuthAttemptCursor(rows.last().at, rows.last().id).toString() else ""),
                "from" to query.from.toString(), "to" to query.to.toString(),
            ))
        }
    }

    /** 잠금 상태(화면 표시용) — `{locked, locked_until, failures}` */
    @GetMapping("/auth/lock")
    fun lock(request: HttpServletRequest): ResponseEntity<out Any> = lock(request, Instant.now())

    internal fun lock(request: HttpServletRequest, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        return answering("lock status") {
            val status = attempts.lockStatus(now)
            ok(mapOf("locked" to status.locked, "locked_until" to status.lockedUntil?.toString(), "failures" to status.failures))
        }
    }

    companion object {
        const val MAX_LIMIT = 200
        const val DEFAULT_LIMIT = 50
        const val MAX_USER_AGENT_LENGTH = 200
        /** 재전송(`replayed:true`)이 보낼 수 있는 결과 — 자격 검증에 이른 시도의 낱말 */
        val REPLAYED_OUTCOMES = listOf(AuthOutcome.SUCCESS, AuthOutcome.FAILURE, AuthOutcome.LOCKED)
        /** 거절 기록의 이유 — 기록의 결과 낱말이 이유와 같다 */
        val REJECTED_OUTCOMES = listOf(AuthOutcome.RATE_LIMITED, AuthOutcome.ORIGIN_REJECTED, AuthOutcome.MALFORMED)
        const val MAX_IP_LENGTH = 64            // IPv6(영역 표시 포함)도 들어가는 길이
        val MAX_RANGE: Duration = Duration.ofDays(90)
    }
}
