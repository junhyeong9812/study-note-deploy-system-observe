package xyz.junproject.observe.logalert

import xyz.junproject.observe.threshold.LogAlerts
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

/**
 * 무수신 판정 — 서비스 · 규칙마다 상태 하나(전체 한 줄은 서비스 [ALL_SERVICES]). 조용함 = 마지막 로그가 기준보다 엄격히 오래거나 받은 기록이 없음.
 * 넘을 때 1회(서비스가 사는 서버가 자원 끊김으로 넘어 있으면 알리지 않고 넘음만 남긴다 — 서버가 돌아온 뒤에도 조용하면 그때 처음 알린다),
 * 넘어 있는 채로 마지막 알림에서 [remindEvery] 가 지나면 `(계속)` 다시 1회(서버가 끊긴 동안은 없음), 다시 받으면 해소 1회(알린 넘음만).
 * 보류([LogJudgeInput.judging] 가 false)면 앞 상태를 그대로 두고 알림을 만들지 않는다. 전체 한 줄은 서버 끊김과 무관하다.
 * 설정에서 빠진 서비스 · 꺼진 전체 한 줄의 상태는 버린다(해소도 알리지 않는다).
 */
class LogSilentJudge(private val settings: LogAlertSettings, private val remindEvery: Duration = Duration.ofHours(1)) {
    fun judge(input: LogJudgeInput, previous: List<LogRuleState>): LogJudgeResult {
        val states = mutableListOf<LogRuleState>()
        val events = mutableListOf<ResourceAlert>()
        fun previousOf(service: String, rule: LogRule) = previous.firstOrNull { it.service == service && it.rule == rule }
        for (watched in settings.services) {
            val step = step(input, previousOf(watched.service, LogRule.SILENT), watched.service, LogRule.SILENT, input.lastReceived[watched.service],
                watched.threshold, watched.server in input.silentServers) { last, resolved ->
                LogAlerts.silent(watched.service, watched.server, last, watched.threshold, input.now, resolved)
            }
            step.state?.let { states += it }
            step.event?.let { events += it }
        }
        settings.allThreshold?.let { threshold ->
            val step = step(input, previousOf(ALL_SERVICES, LogRule.SILENT_ALL), ALL_SERVICES, LogRule.SILENT_ALL, input.lastReceivedAny, threshold,
                serverSilent = false) { last, resolved -> LogAlerts.silentAll(last, threshold, input.now, resolved) }
            step.state?.let { states += it }
            step.event?.let { events += it }
        }
        return LogJudgeResult(states, events)
    }

    private class Step(val state: LogRuleState?, val event: ResourceAlert? = null)

    /** 한 서비스(또는 전체 한 줄)의 판정 — [alert] 는 (마지막 로그, 해소인가) → 몸 */
    private fun step(input: LogJudgeInput, before: LogRuleState?, service: String, rule: LogRule, last: Instant?, threshold: Duration,
                     serverSilent: Boolean, alert: (Instant?, Boolean) -> ResourceAlert): Step {
        if (!input.judging) return Step(before)                                       // 보류 — 앞 상태 그대로
        val now = input.now
        val quiet = last == null || Duration.between(last, now) > threshold
        val firing = before?.firing == true
        return when {
            !firing && quiet && serverSilent -> Step(LogRuleState(service, rule, true, now, null, last))           // 넘음만 — 아직 알리지 않는다
            !firing && quiet -> Step(LogRuleState(service, rule, true, now, now, last), alert(last, false))
            !firing -> Step(LogRuleState(service, rule, false, null, before?.lastNotifiedAt, last))
            !quiet && before!!.lastNotifiedAt != null -> Step(LogRuleState(service, rule, false, null, now, last), alert(last, true))
            !quiet -> Step(LogRuleState(service, rule, false, null, null, last))                                     // 알린 적 없는 넘음은 해소를 알리지 않는다
            serverSilent -> Step(before!!.copy(lastReceivedAt = last))                                                // 서버가 끊긴 동안은 알리지 않는다
            before!!.lastNotifiedAt == null -> Step(before.copy(lastNotifiedAt = now, lastReceivedAt = last), alert(last, false))
            Duration.between(before.lastNotifiedAt, now) >= remindEvery -> {
                val base = alert(last, false)
                Step(before.copy(lastNotifiedAt = now, lastReceivedAt = last),
                    base.copy(title = "(계속) ${base.title}", detail = "${base.detail}\n처음 넘은 시각: ${before.since}"))
            }
            else -> Step(before.copy(lastReceivedAt = last))
        }
    }

    companion object {
        /** 전체 한 줄의 상태가 쓰는 서비스 자리 — 설정은 이 이름의 서비스를 받지 않는다 */
        const val ALL_SERVICES = "*"
    }
}
