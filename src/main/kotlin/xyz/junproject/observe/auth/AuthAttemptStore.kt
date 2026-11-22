package xyz.junproject.observe.auth

import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/** PostgreSQL 저장소 — 판정은 [AuthAttemptPolicy.judge] 를 한 트랜잭션 안에서 쓴다 */
class AuthAttemptStore(private val jdbc: JdbcTemplate, private val policy: AuthAttemptPolicy = AuthAttemptPolicy()) : AuthAttempts {
    override fun createSchema(): Unit = TODO()
    override fun decide(request: AuthAttemptRequest): AuthVerdict = TODO()
    override fun recordRejected(attempt: AuthRejectedAttempt): Unit = TODO()
    override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> = TODO()
    override fun lockStatus(now: Instant): AuthLockStatus = TODO()
    override fun deleteOlderThan(cutoff: Instant): Int = TODO()
}
