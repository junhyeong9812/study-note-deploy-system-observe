package xyz.junproject.observe.logalert

import xyz.junproject.observe.threshold.LogRule
import xyz.junproject.observe.threshold.ResourceAlert
import java.time.Duration
import java.time.Instant

/**
 * 로그 규칙 하나의 상태(`log_rule_state` 한 줄 — 열쇠 = 서비스 · 규칙). 전체 한 줄은 서비스 [LogSilentJudge.ALL_SERVICES].
 * [since] 는 넘음으로 바뀐 판정 시각, [lastNotifiedAt] 은 마지막으로 알림을 만든 시각(넘음을 아직 알리지 않았으면 null), [lastReceivedAt] 은 그 판정에서 본 마지막 로그의 시각
 */
data class LogRuleState(
    val service: String,
    val rule: LogRule,
    val firing: Boolean,
    val since: Instant?,
    val lastNotifiedAt: Instant?,
    val lastReceivedAt: Instant?,
)

/**
 * 판정의 입력 — [lastReceived] 는 감시하는 서비스마다 마지막으로 받은 로그의 시각(없으면 null), [lastReceivedAny] 는 어느 서비스든 마지막 로그의 시각,
 * [judging] 은 지금 판정하는가(보류면 false), [silentServers] 는 자원 끊김이 넘어 있는 서버
 */
data class LogJudgeInput(
    val now: Instant,
    val lastReceived: Map<String, Instant?>,
    val lastReceivedAny: Instant?,
    val judging: Boolean,
    val silentServers: Set<String>,
)

/** 판정 결과 — [states] 는 판정 뒤의 상태 전부, [events] 는 이번에 만든 알림 */
data class LogJudgeResult(val states: List<LogRuleState>, val events: List<ResourceAlert>)

/** 무수신 판정. 뼈대(03): 아직 없다 */
class LogSilentJudge(private val settings: LogAlertSettings, private val remindEvery: Duration = Duration.ofHours(1)) {
    fun judge(input: LogJudgeInput, previous: List<LogRuleState>): LogJudgeResult = TODO("03: 무수신 판정")

    companion object {
        /** 전체 한 줄의 상태가 쓰는 서비스 자리 — 설정은 이 이름의 서비스를 받지 않는다 */
        const val ALL_SERVICES = "*"
    }
}
