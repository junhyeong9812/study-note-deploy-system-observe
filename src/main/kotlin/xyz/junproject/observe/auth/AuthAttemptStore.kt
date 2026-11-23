package xyz.junproject.observe.auth

import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.sql.ResultSet
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/**
 * PostgreSQL 저장소 — 판정은 [AuthAttemptPolicy.judge] 를 한 트랜잭션 안에서 쓴다.
 *
 * 표 둘(추가만 — `log_entry` 는 건드리지 않는다):
 * - `auth_attempt`: 시도 한 건 = 한 행, 기본 키가 `attempt_id`. 판정 경로로 들어온 행은 돌려준 판정(`verdict_*`)을 함께 남겨
 *   같은 ID 의 재전송에 그대로 돌려준다. 거절 행(rate_limited·origin_rejected·malformed)은 판정 칸이 비어 있다.
 * - `auth_lock_state`: 유일 계정의 잠금 상태 한 행(`account = 1`). 판정은 이 행을 `FOR UPDATE` 로 잡고 시작한다 —
 *   판정끼리 이 행에서 한 줄로 서므로 동시에 들어온 실패도 횟수가 겹치지 않고, 잠금 시작은 한 번뿐이며,
 *   같은 ID 의 동시 재전송도 앞선 판정이 커밋된 뒤에 그 행을 보게 되어 유일 제약 충돌이 나지 않는다.
 */
class AuthAttemptStore(private val jdbc: JdbcTemplate, private val policy: AuthAttemptPolicy = AuthAttemptPolicy()) : AuthAttempts {
    private val transaction = TransactionTemplate(DataSourceTransactionManager(requireNotNull(jdbc.dataSource) { "JdbcTemplate 에 DataSource 가 없다" }))

    /**
     * `IF NOT EXISTS` 라도 두 자리가 **동시에 처음** 만들면 한쪽이 고유 제약 위반으로 실패할 수 있다(`log_entry` 와 같은 한계).
     * 그때는 그 호출이 실패로 끝나고 다음 호출이 다시 만든다 — 두 번째에는 이미 있어 그대로 지나간다.
     */
    override fun createSchema() {
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS auth_attempt (
                 id                   uuid        PRIMARY KEY,
                 at                   timestamptz NOT NULL,
                 outcome              text        NOT NULL,
                 ip                   text        NOT NULL,
                 user_agent           text        NOT NULL,
                 replayed             boolean     NOT NULL,
                 verdict_allowed      boolean,
                 verdict_locked_until timestamptz,
                 verdict_lock_started boolean,
                 verdict_failures     integer)"""
        )
        jdbc.execute("CREATE INDEX IF NOT EXISTS auth_attempt_at ON auth_attempt (at DESC, id DESC)")
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS auth_lock_state (
                 account      smallint    PRIMARY KEY CHECK (account = 1),
                 failures     integer     NOT NULL,
                 locked_until timestamptz)"""
        )
        jdbc.update(INSERT_LOCK_STATE)
    }

    override fun decide(request: AuthAttemptRequest): AuthVerdict = transaction.execute {
        jdbc.update(INSERT_LOCK_STATE)   // 상태 행이 없어졌어도 판정이 멈추지 않게 — 있으면 아무것도 하지 않는다
        val state = jdbc.queryForObject(
            "SELECT failures, locked_until FROM auth_lock_state WHERE account = 1 FOR UPDATE",
            { row, _ -> AuthLockState(row.getInt("failures"), row.instantOrNull("locked_until")) },
        )!!
        // 상태 행을 잡은 뒤에 본다 — 같은 ID 의 앞선 판정이 있었다면 이미 커밋되어 보인다
        recordedVerdict(request.attemptId)?.let { return@execute it }

        val verdict = if (request.replayed) {
            // 감사 기록만 — 잠금 계산에 넣지 않고 상태도 바꾸지 않는다. 결과는 어드민이 임시 제한으로 실제 내린 판정 그대로(세부 2).
            // 지금 로그인을 허용하는 답이 아니므로 allowed 는 거짓
            val outcome = requireNotNull(request.outcome) { "replayed 시도에는 outcome 이 있어야 한다" }
            require(outcome in REPLAYED_OUTCOMES) { "replayed 의 outcome 은 success·failure·locked 중 하나" }
            val status = policy.status(state, request.at)
            AuthVerdict(false, outcome, status.lockedUntil, false, status.failures)
        } else {
            val judgement = policy.judge(state, request.credentialOk, request.at)
            if (judgement.nextState != state) {
                jdbc.update("UPDATE auth_lock_state SET failures = ?, locked_until = ? WHERE account = 1",
                    judgement.nextState.failures, judgement.nextState.lockedUntil?.let(::timestamp))
            }
            judgement.verdict
        }
        val inserted = jdbc.update(
            """INSERT INTO auth_attempt (id, at, outcome, ip, user_agent, replayed,
                                         verdict_allowed, verdict_locked_until, verdict_lock_started, verdict_failures)
               VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (id) DO NOTHING""",
            request.attemptId, timestamp(request.at), verdict.outcome.wireName, request.ip, request.userAgent, request.replayed,
            verdict.allowed, verdict.lockedUntil?.let(::timestamp), verdict.lockStarted, verdict.failures,
        )
        // 판정 행이면 위에서 돌아갔다 — 여기서 부딪히면 같은 ID 가 속도 제한 거절로 먼저 기록된 것이다. 상태 변경째 되돌린다
        if (inserted == 0) throw DuplicateKeyException("attempt_id 가 이미 판정이 아닌 기록으로 있다")
        verdict
    }!!

    private fun recordedVerdict(id: UUID): AuthVerdict? = jdbc.query(
        """SELECT outcome, verdict_allowed, verdict_locked_until, verdict_lock_started, verdict_failures
           FROM auth_attempt WHERE id = ?""",
        { row, _ ->
            val allowed = row.getBoolean("verdict_allowed")
            if (row.wasNull()) throw DuplicateKeyException("attempt_id 가 이미 판정이 아닌 기록으로 있다")
            AuthVerdict(allowed, outcome(row.getString("outcome")), row.instantOrNull("verdict_locked_until"),
                row.getBoolean("verdict_lock_started"), row.getInt("verdict_failures"))
        },
        id,
    ).firstOrNull()

    override fun recordRejected(attempt: AuthRejectedAttempt) {
        jdbc.update(
            """INSERT INTO auth_attempt (id, at, outcome, ip, user_agent, replayed) VALUES (?,?,?,?,?,false)
               ON CONFLICT (id) DO NOTHING""",
            attempt.attemptId, timestamp(attempt.at), rejectedOutcome(attempt.reason).wireName, attempt.ip, attempt.userAgent,
        )
    }

    override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> {
        val conditions = mutableListOf("at >= ?", "at <= ?")
        val arguments = mutableListOf<Any>(timestamp(query.from), timestamp(query.to))
        query.before?.let { conditions += "(at, id) < (?, ?)"; arguments += timestamp(it.at); arguments += it.id }
        arguments += query.limit
        return jdbc.query(
            """SELECT id, at, outcome, ip, user_agent, replayed FROM auth_attempt
               WHERE ${conditions.joinToString(" AND ")}
               ORDER BY at DESC, id DESC LIMIT ?""",
            { row, _ ->
                AuthAttemptRow(row.getObject("id", UUID::class.java), row.instantOrNull("at")!!, outcome(row.getString("outcome")),
                    row.getString("ip"), row.getString("user_agent"), row.getBoolean("replayed"))
            },
            *arguments.toTypedArray(),
        )
    }

    override fun lockStatus(now: Instant): AuthLockStatus {
        val state = jdbc.query(
            "SELECT failures, locked_until FROM auth_lock_state WHERE account = 1",
            { row, _ -> AuthLockState(row.getInt("failures"), row.instantOrNull("locked_until")) },
        ).firstOrNull() ?: AuthLockState()
        return policy.status(state, now)
    }

    override fun deleteOlderThan(cutoff: Instant): Int = jdbc.update("DELETE FROM auth_attempt WHERE at < ?", timestamp(cutoff))

    private companion object {
        const val INSERT_LOCK_STATE = "INSERT INTO auth_lock_state (account, failures, locked_until) VALUES (1, 0, NULL) ON CONFLICT (account) DO NOTHING"

        fun timestamp(instant: Instant): OffsetDateTime = OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)

        fun ResultSet.instantOrNull(column: String): Instant? = getObject(column, OffsetDateTime::class.java)?.toInstant()

        val REPLAYED_OUTCOMES = setOf(AuthOutcome.SUCCESS, AuthOutcome.FAILURE, AuthOutcome.LOCKED)

        /** 거절 이유 → 기록의 결과 낱말(같은 글자) — 세 이유 밖이면 계약 위반 */
        fun rejectedOutcome(reason: String): AuthOutcome =
            setOf(AuthOutcome.RATE_LIMITED, AuthOutcome.ORIGIN_REJECTED, AuthOutcome.MALFORMED).firstOrNull { it.wireName == reason }
                ?: throw IllegalArgumentException("거절 이유가 아니다: $reason")

        fun outcome(wireName: String): AuthOutcome =
            AuthOutcome.entries.firstOrNull { it.wireName == wireName } ?: throw IllegalStateException("알 수 없는 결과 낱말: $wireName")
    }
}
