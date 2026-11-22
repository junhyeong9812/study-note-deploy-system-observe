package xyz.junproject.observe.auth

import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 잠금 수치 — 명세 ②: 계정 기준 연속 [failureLimit] 회 실패 → [lockDuration] 잠금, 시도 기록 보관 [retention].
 * 설정으로 바꿀 수 있게 값으로 두되, 기본값이 명세 그대로다.
 */
data class AuthAttemptPolicy(
    val failureLimit: Int = 5,
    val lockDuration: Duration = Duration.ofMinutes(15),
    val retention: Duration = Duration.ofDays(90),
) {
    /**
     * 판정 규칙(순수 함수) — 저장소가 한 트랜잭션 안에서 잠금 상태 행을 잡고([state]) 이 함수로 다음 상태와 판정을 얻어 쓴다.
     * 잠금 중(`at < lockedUntil`)이면 자격이 맞아도 거절하고 상태를 바꾸지 않는다. 잠금이 풀린 뒤(`at >= lockedUntil`)에는 0 부터 센다.
     */
    fun judge(state: AuthLockState, credentialOk: Boolean, at: Instant): AuthJudgement = TODO()
}

/** 유일 계정의 잠금 상태 — 연속 실패 횟수와 잠금 끝 시각(없으면 null) */
data class AuthLockState(val failures: Int = 0, val lockedUntil: Instant? = null)

/** 판정 규칙의 결과 — 돌려줄 판정과 저장할 다음 상태 */
data class AuthJudgement(val verdict: AuthVerdict, val nextState: AuthLockState)

/** 기록의 결과 낱말 — 응답·목록에 나가는 글자 그대로 */
enum class AuthOutcome(val wireName: String) {
    SUCCESS("success"),
    FAILURE("failure"),
    LOCKED("locked"),
    RATE_LIMITED("rate_limited"),
}

/** `POST /auth/attempts` 의 본문 — 값 검증은 컨트롤러에서 끝낸다 */
data class AuthAttemptRequest(
    val attemptId: UUID,
    val at: Instant,
    val credentialOk: Boolean,
    val ip: String,
    val userAgent: String,
    val replayed: Boolean = false,
)

/** `POST /auth/attempts` 의 응답 */
data class AuthVerdict(
    val allowed: Boolean,
    val outcome: AuthOutcome,
    val lockedUntil: Instant?,
    val lockStarted: Boolean,
    val failures: Int,
)

/** `POST /auth/attempts/rejected` 의 본문 — 속도 제한으로 거절한 시도. 기록만 한다 */
data class AuthRejectedAttempt(
    val attemptId: UUID,
    val at: Instant,
    val reason: String,
    val ip: String,
    val userAgent: String,
)

/** 목록의 한 행 — 입력한 ID·비밀번호는 저장하지 않으므로 여기에도 없다 */
data class AuthAttemptRow(
    val id: UUID,
    val at: Instant,
    val outcome: AuthOutcome,
    val ip: String,
    val userAgent: String,
    val replayed: Boolean,
)

/** 목록 이어 보기의 기준 — 응답의 `next_before` 글자와 오간다. 이 항목보다 오래된 것부터 */
data class AuthAttemptCursor(val at: Instant, val id: UUID) {
    /** `next_before` 로 나가는 글자 — [parse] 가 되읽는다 */
    override fun toString(): String = TODO()

    companion object {
        /** 응답의 `next_before` 꼴이 아니면 null */
        fun parse(text: String): AuthAttemptCursor? = TODO()
    }
}

/** 목록 조건 — 값 검증은 API 경계에서 끝낸다 */
data class AuthAttemptQuery(
    val from: Instant,
    val to: Instant,
    val before: AuthAttemptCursor? = null,
    val limit: Int = 50,
)

/** `GET /auth/lock` 의 응답(화면 표시용 — 판정에는 쓰지 않는다) */
data class AuthLockStatus(val locked: Boolean, val lockedUntil: Instant?, val failures: Int)

/** 시도 기록·잠금 판정 저장소의 면 */
interface AuthAttempts {
    /** 표를 만든다(있으면 그대로) — 추가만, 기존 `log_entry` 는 건드리지 않는다. 실패는 예외. */
    fun createSchema()

    /**
     * 기록 · 연속 실패 횟수 갱신 · 잠금 판정을 PostgreSQL 트랜잭션 한 건으로. 같은 [AuthAttemptRequest.attemptId] 가 이미 있으면
     * 새로 세지 않고 처음의 판정을 그대로 돌려준다. `replayed` 는 기록만 하고 잠금 상태를 바꾸지 않는다. 실패는 예외.
     */
    fun decide(request: AuthAttemptRequest): AuthVerdict

    /** 속도 제한 거절을 기록만 한다(잠금 계산 없음). 같은 ID 가 이미 있으면 그대로 둔다. 실패는 예외. */
    fun recordRejected(attempt: AuthRejectedAttempt)

    /** 최신순(at 내림차순) — [AuthAttemptQuery.before] 보다 오래된 것부터 [AuthAttemptQuery.limit] 건 */
    fun list(query: AuthAttemptQuery): List<AuthAttemptRow>

    /** [now] 기준 잠금 상태 */
    fun lockStatus(now: Instant): AuthLockStatus

    /** [cutoff] 보다 오래된 시도 기록을 지운다. 지운 행 수를 돌려준다. */
    fun deleteOlderThan(cutoff: Instant): Int
}
