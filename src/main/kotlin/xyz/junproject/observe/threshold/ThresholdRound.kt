package xyz.junproject.observe.threshold

import org.slf4j.LoggerFactory
import xyz.junproject.observe.metric.MetricSampleSource
import java.time.Instant

/**
 * 판정과 보내기 한 바퀴의 결과(헬스용). [judged] 는 잠금을 얻어 판정했는가, [judgeFailure] · [dispatchFailure] 는 그 단계의 예외 종류 이름
 * (판정 단계가 실패하면 [judged] 는 false). [dispatch] 는 보내기 단계가 끝까지 돌았을 때의 결과.
 */
data class ThresholdRoundResult(
    val ranAt: Instant,
    val judged: Boolean,
    val eventsCreated: Int,
    val judgeFailure: String?,
    val dispatch: DispatchOutcome?,
    val dispatchFailure: String?,
)

/**
 * 판정과 보내기(§11.4 · §15) — 둘은 따로 도는 예약 작업이다: [judgeOnce](30초마다) · [dispatchOnce](보내기 간격마다). [runOnce] 는 둘을 차례로.
 * 판정: 판정 잠금을 얻은 자리만 ① 표본 읽기(측정 시각 `(now − judge.lookback, now]`) · 서버별 마지막 수신 시각 ② 판정 · 기록([RuleStateStore.judgeRound]).
 * 어떤 것도 예외를 내지 않는다. 결과는 [lastResult](판정) · [lastDispatchFailure](보내기), 마지막으로 판정한 시각은 [lastJudgedAt],
 * 보낼 표의 건수는 바퀴가 끝날 때 세어 [lastAlertCounts] 에 둔다(헬스는 이 값만 읽는다).
 */
class ThresholdRound(
    private val samples: MetricSampleSource,
    private val states: RuleStateStore,
    private val judge: ThresholdJudge,
    private val dispatcher: AlertDispatcher,
    private val queue: AlertQueue,
    private val expectedServers: Set<String>,
    /** 끊김 판정의 시작 조건(§15.1) — 없으면 늘 판정한다 */
    private val silentGate: SilentGate? = null,
) {
    @Volatile var lastResult: ThresholdRoundResult? = null; private set
    @Volatile var lastJudgedAt: Instant? = null; private set

    /**
     * 마지막 바퀴(판정이든 보내기든)가 끝날 때 판정 · 보내기용 연결로 읽어 둔 보낼 표의 건수.
     * 헬스는 이 값만 읽는다(헬스가 판정 · 보내기용 연결을 쓰거나 표를 만들지 않게). 아직 못 읽었거나 마지막에 읽지 못했으면 null
     */
    @Volatile var lastAlertCounts: AlertCounts? = null; private set

    /** 마지막 보내기 바퀴가 실패했으면 그 예외 종류 이름, 성공했으면 null(헬스의 `last_dispatch_failure`) */
    @Volatile var lastDispatchFailure: String? = null; private set

    private val log = LoggerFactory.getLogger("observe")

    /**
     * 판정만 한 바퀴(§15 — 보내기는 [dispatchOnce] 가 따로 돈다). 표본은 **판정 잠금을 얻은 뒤에만** 읽는다(§15) —
     * 잠금을 못 얻으면 [MetricSampleSource] 를 부르지 않는다. 끊김 판정 여부는 [silentGate]. 예외를 내지 않는다.
     * 결과의 `dispatch` 는 null(보내기를 하지 않았다).
     */
    fun judgeOnce(now: Instant): ThresholdRoundResult {
        val silentJudging = silentJudging(now)
        val judged = runCatching {
            states.judgeRound(now) { previous ->
                val recent = samples.samplesMeasuredBetween(now.minus(judge.lookback), now)
                val received = samples.lastReceivedOfEveryServer()
                judge.judge(JudgeInput(now, recent, received, expectedServers, silentJudging), previous)
            }
        }.onFailure { logFailure("judge", it) }
        val outcome = judged.getOrNull()
        if (outcome?.judged == true) lastJudgedAt = now
        if (outcome != null && outcome.eventsCreated > 0) log.info("observe:observe:threshold round created {} alerts", outcome.eventsCreated)
        countAlerts()
        return ThresholdRoundResult(now, outcome?.judged == true, outcome?.eventsCreated ?: 0, judged.exceptionOrNull()?.javaClass?.simpleName,
            null, null).also { lastResult = it }
    }

    /** 보내기만 한 바퀴(§15 — 판정과 따로 돈다). 예외를 내지 않는다 — 실패면 null 이고 [lastResult] 의 실패 칸은 건드리지 않는다 */
    fun dispatchOnce(now: Instant): DispatchOutcome? {
        val dispatched = runCatching { dispatcher.dispatch(now) }.onFailure { logFailure("dispatch", it) }
        lastDispatchFailure = dispatched.exceptionOrNull()?.javaClass?.simpleName
        val dispatch = dispatched.getOrNull()
        if (dispatch != null && dispatch.delivered + dispatch.retrying + dispatch.rejected > 0) {
            log.info("observe:observe:alerts delivered {} · retrying {} · rejected {}", dispatch.delivered, dispatch.retrying, dispatch.rejected)
        }
        // 이 판이 모르는 규칙 글자의 줄(섞인 판에서 새 판이 쓴 줄) — 잠금을 얻은 바퀴마다 한 줄. 줄은 남아 그 규칙을 아는 판이 보낸다.
        // 규칙 글자는 다른 판이 쓴 표의 값이라 로그에 찍을 때만 다듬는다: 제어 문자는 '?', 글자마다 64자, 규칙 글자 순으로 앞의 10가지
        val skipped = dispatch?.skippedUnknownRules.orEmpty()
        if (skipped.isNotEmpty()) {
            val printed = skipped.toSortedMap().entries.take(LOGGED_RULES)
                .joinToString(", ") { "${printableRule(it.key)}=${it.value}" }
            val more = if (skipped.size > LOGGED_RULES) " (+${skipped.size - LOGGED_RULES} more)" else ""
            log.warn("observe:observe:alerts skipped unknown rule lines (left for a version that knows the rule): {}{} · total {}",
                printed, more, skipped.values.sum())
        }
        countAlerts()
        return dispatch
    }

    /** 지금 끊김 규칙을 판정하는가(헬스의 `silent_judging`) — [silentGate] 가 없으면 true */
    fun silentJudging(now: Instant): Boolean = silentGate?.active(now) ?: true

    /** 판정 다음 보내기 — 판정이 실패해도 보내기는 돌고(앞 바퀴에 남은 알림), 보내기가 실패해도 판정 결과는 남는다 */
    fun runOnce(now: Instant): ThresholdRoundResult {
        val judged = judgeOnce(now)
        val dispatch = dispatchOnce(now)
        return judged.copy(dispatch = dispatch, dispatchFailure = lastDispatchFailure).also { lastResult = it }
    }

    private fun countAlerts() {
        lastAlertCounts = runCatching { queue.alertCounts() }.getOrNull()
    }

    /** 실패의 문구는 서버 로그에만(헬스에는 종류 이름만). 알림 서버의 주소 · 비밀값은 보내는 쪽이 문구에 싣지 않는다 */
    private fun logFailure(stage: String, error: Throwable) =
        log.error("observe:observe:threshold {} failed: {}: {}", stage, error.javaClass.simpleName, error.message?.take(200))

    /** 보낼 표의 건수(헬스용) — 읽지 못하면 null */
    fun alertCounts(): AlertCounts? = runCatching { queue.alertCounts() }.getOrNull()

    private companion object {
        const val LOGGED_RULES = 10
        const val LOGGED_RULE_LENGTH = 64

        /** 로그에 찍는 규칙 글자 — 줄바꿈 등으로 가짜 로그 줄을 만들 수 없게 제어 문자를 '?' 로, 길이를 [LOGGED_RULE_LENGTH] 까지 */
        fun printableRule(rule: String): String =
            rule.take(LOGGED_RULE_LENGTH).map { if (Character.isISOControl(it)) '?' else it }.joinToString("")
    }
}
