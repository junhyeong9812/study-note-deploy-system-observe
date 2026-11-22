package xyz.junproject.observe.auth

import jakarta.servlet.http.HttpServletRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.time.Instant

/**
 * 로그인 시도 경로 — 전부 `X-Observe-Secret`(설정이 비면 전부 거절). 봉투는 `{success,data}` / `{success:false,error}`.
 * 본문은 맵으로 받아 꼴이 틀리면 422 `invalid_request`. 저장소 오류는 503 `query_failed`(예외 문구는 응답에 넣지 않는다).
 */
@RestController
class AuthAttemptController(
    private val attempts: AuthAttempts,
    @Value("\${observe.secret:}") private val secret: String,
) {
    /** 판정 — `{attempt_id, at, credential_ok, ip, user_agent, replayed?}` → `{allowed, outcome, locked_until, lock_started, failures}` */
    @PostMapping("/auth/attempts")
    fun decide(request: HttpServletRequest, @RequestBody(required = false) body: Map<String, Any?>?): ResponseEntity<out Any> = TODO()

    /** 속도 제한 거절의 기록 — `{attempt_id, at, reason:"rate_limited", ip, user_agent}` */
    @PostMapping("/auth/attempts/rejected")
    fun rejected(request: HttpServletRequest, @RequestBody(required = false) body: Map<String, Any?>?): ResponseEntity<out Any> = TODO()

    /** 목록 — 최신순, `{attempts:[{id,at,outcome,ip,user_agent,replayed}], next_before}` */
    @GetMapping("/auth/attempts")
    fun list(
        request: HttpServletRequest,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
        @RequestParam(required = false) before: String?,
        @RequestParam(required = false) limit: String?,
    ): ResponseEntity<out Any> = list(request, from, to, before, limit, Instant.now())

    internal fun list(request: HttpServletRequest, from: String?, to: String?, before: String?, limit: String?, now: Instant): ResponseEntity<out Any> = TODO()

    /** 잠금 상태(화면 표시용) — `{locked, locked_until, failures}` */
    @GetMapping("/auth/lock")
    fun lock(request: HttpServletRequest): ResponseEntity<out Any> = lock(request, Instant.now())

    internal fun lock(request: HttpServletRequest, now: Instant): ResponseEntity<out Any> = TODO()

    companion object {
        const val MAX_LIMIT = 200
        const val DEFAULT_LIMIT = 50
        const val MAX_USER_AGENT_LENGTH = 200
        val MAX_RANGE: Duration = Duration.ofDays(90)
    }
}
