package xyz.junproject.observe.threshold

import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/** 판정 한 바퀴의 결과 — 잠금을 얻지 못했으면(다른 자리가 판정 중) [judged] 는 false 이고 아무것도 바꾸지 않았다 */
data class RoundOutcome(val judged: Boolean, val eventsCreated: Int)

/** 보낼 표의 한 줄 — [attempts] 는 지금까지 보낸 횟수, [lastFailure] 는 마지막 실패 사유 */
data class QueuedAlert(val id: Long, val alert: ResourceAlert, val attempts: Int, val lastFailure: String?)

/** 보낼 표의 건수 — [undelivered] 는 아직 보내지 못한 것(다시 보낼 것), [rejected] 는 알림 서버가 거절해 다시 보내지 않는 것 */
data class AlertCounts(val undelivered: Long, val rejected: Long)

/** `GET /metrics/latest` 의 `firing[]` 한 줄 — `{server, rule, target, severity, since, last_value}` (target 은 재시작 규칙의 컨테이너 이름, 그 밖은 null) */
data class FiringRule(val server: String, val rule: String, val target: String?, val severity: String, val since: Instant, val lastValue: Double?)

/** 지금 넘어 있는 규칙(서버 · 규칙 · 대상 순) */
interface FiringSource {
    fun firing(): List<FiringRule>
}

/** 판정 상태 저장 — 한 바퀴가 DB 잠금 · 상태 · 보낼 알림을 **한 트랜잭션**에 */
interface RuleStateStore {
    /**
     * 판정 잠금(자리 둘 중 한쪽만 — PostgreSQL 잠금)을 **기다리지 않고** 시도해, 얻으면 같은 트랜잭션 안에서 앞 상태를 읽어 [judge] 에 넘기고
     * 돌려받은 상태 전부와 알림을 기록한 뒤 커밋한다. 얻지 못하면 [judge] 를 부르지 않고 `judged = false`.
     * 같은 [ResourceAlert.eventId] 의 알림은 한 번만 남는다. 도중의 어떤 실패도 상태와 알림을 함께 되돌리고 예외로 낸다.
     */
    fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome
}

/** 보낼 표 — 보내는 쪽([AlertDispatcher])이 쓰는 면 */
interface AlertQueue {
    /** 아직 보내지 못했고 거절되지 않은 알림 — 오래된 것부터 [limit] 건 */
    fun pendingAlerts(limit: Int): List<QueuedAlert>
    /** 알림 서버가 받았다 — 다시 보내지 않는다 */
    fun markDelivered(id: Long, at: Instant)
    /** 보내지 못했다 — 남겨 두고 다음 바퀴에 다시 보낸다(횟수 · 사유를 남긴다) */
    fun markRetry(id: Long, at: Instant, reason: String)
    /** 알림 서버가 거절했다(다시 보내도 같은 답) — 다시 보내지 않고 거절 건수로 센다 */
    fun markRejected(id: Long, at: Instant, reason: String)
    /** 보내기 잠금(자리 둘 중 한쪽만)을 기다리지 않고 시도해 얻으면 [block] 을 돌리고 그 값을, 얻지 못하면 null */
    fun <T> whileHoldingDispatchLock(block: () -> T): T?
    fun alertCounts(): AlertCounts
}

/**
 * PostgreSQL 저장소 — 표는 추가만(로그 · 로그인 시도 · 지표 표는 건드리지 않는다). 표 이름과 꼴은 구현이 정한다.
 * [failpoint] 는 테스트의 이음매 — 한 바퀴의 트랜잭션 안에서 상태를 쓴 직후 `"states_written"`, 알림을 쓴 직후 `"events_written"` 으로
 * 부른다(예외를 던지면 그 바퀴가 통째로 되돌려져야 한다). 운영에서는 아무것도 하지 않는다.
 */
class ThresholdStore(
    private val jdbc: JdbcTemplate,
    private val failpoint: (String) -> Unit = {},
) : RuleStateStore, AlertQueue, FiringSource {
    /** 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema(): Unit = TODO("not implemented")

    /** 저장된 상태 전부(테스트 · 확인용) */
    fun states(): List<RuleState> = TODO("not implemented")

    override fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome = TODO("not implemented")
    override fun pendingAlerts(limit: Int): List<QueuedAlert> = TODO("not implemented")
    override fun markDelivered(id: Long, at: Instant): Unit = TODO("not implemented")
    override fun markRetry(id: Long, at: Instant, reason: String): Unit = TODO("not implemented")
    override fun markRejected(id: Long, at: Instant, reason: String): Unit = TODO("not implemented")
    override fun <T> whileHoldingDispatchLock(block: () -> T): T? = TODO("not implemented")
    override fun alertCounts(): AlertCounts = TODO("not implemented")
    override fun firing(): List<FiringRule> = TODO("not implemented")
}
