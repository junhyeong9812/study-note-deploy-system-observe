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
 * 30초마다 도는 한 바퀴(§11.4) — ① 표본 읽기(측정 시각 `(now − judge.lookback, now]`) · 서버별 마지막 수신 시각 ② [RuleStateStore.judgeRound] 로
 * 판정 ③ [AlertDispatcher.dispatch] 로 보내기. 판정이 실패해도 보내기는 돌고(앞 바퀴에 남은 알림), 보내기가 실패해도 판정 결과는 남는다.
 * 예외를 내지 않는다. 결과는 [lastResult], 마지막으로 잠금을 얻어 판정에 성공한 시각은 [lastJudgedAt].
 */
class ThresholdRound(
    private val samples: MetricSampleSource,
    private val states: RuleStateStore,
    private val judge: ThresholdJudge,
    private val dispatcher: AlertDispatcher,
    private val queue: AlertQueue,
    private val expectedServers: Set<String>,
    /** 끊김 판정의 시작 조건(§15.1) — 없으면 늘 판정한다(리뷰 루프 1 — 구현 전) */
    private val silentGate: SilentGate? = null,
) {
    @Volatile var lastResult: ThresholdRoundResult? = null; private set
    @Volatile var lastJudgedAt: Instant? = null; private set

    /**
     * 리뷰 루프 1(§15 · §15.1 — 구현 전): 마지막 바퀴(판정이든 보내기든)가 끝날 때 판정 · 보내기용 연결로 읽어 둔 보낼 표의 건수.
     * 헬스는 이 값만 읽는다(헬스가 판정 · 보내기용 연결을 쓰거나 표를 만들지 않게). 아직 못 읽었으면 null
     */
    @Volatile var lastAlertCounts: AlertCounts? = null; private set

    /**
     * 판정만 한 바퀴(§15 — 보내기는 [dispatchOnce] 가 따로 돈다. 구현 전). 표본은 **판정 잠금을 얻은 뒤에만** 읽는다(§15) —
     * 잠금을 못 얻으면 [MetricSampleSource] 를 부르지 않는다. 끊김 판정 여부는 [silentGate]. 예외를 내지 않는다.
     * 결과의 `dispatch` 는 null(보내기를 하지 않았다).
     */
    fun judgeOnce(now: Instant): ThresholdRoundResult = TODO("not implemented")

    /** 보내기만 한 바퀴(§15 — 판정과 따로 돈다. 구현 전). 예외를 내지 않는다 — 실패면 null 이고 [lastResult] 의 실패 칸은 건드리지 않는다 */
    fun dispatchOnce(now: Instant): DispatchOutcome? = TODO("not implemented")

    /** 지금 끊김 규칙을 판정하는가(헬스의 `silent_judging`) — [silentGate] 가 없으면 true (구현 전) */
    fun silentJudging(now: Instant): Boolean = TODO("not implemented")

    private val log = LoggerFactory.getLogger("observe")

    fun runOnce(now: Instant): ThresholdRoundResult {
        // 판정과 보내기는 서로 따로 — 한쪽의 실패가 다른 쪽을 막지 않는다. Throwable 까지 잡는다(예약 실행을 죽이지 않게)
        val judged = runCatching {
            val recent = samples.samplesMeasuredBetween(now.minus(judge.lookback), now)
            val received = samples.lastReceivedOfEveryServer()
            states.judgeRound(now) { previous -> judge.judge(JudgeInput(now, recent, received, expectedServers), previous) }
        }.onFailure { logFailure("judge", it) }
        val dispatched = runCatching { dispatcher.dispatch(now) }.onFailure { logFailure("dispatch", it) }
        val outcome = judged.getOrNull()
        if (outcome?.judged == true) lastJudgedAt = now
        val dispatch = dispatched.getOrNull()
        if (outcome != null && outcome.eventsCreated > 0) log.info("observe:observe:threshold round created {} alerts", outcome.eventsCreated)
        if (dispatch != null && dispatch.delivered + dispatch.retrying + dispatch.rejected > 0) {
            log.info("observe:observe:alerts delivered {} · retrying {} · rejected {}", dispatch.delivered, dispatch.retrying, dispatch.rejected)
        }
        return ThresholdRoundResult(now, outcome?.judged == true, outcome?.eventsCreated ?: 0, judged.exceptionOrNull()?.javaClass?.simpleName,
            dispatch, dispatched.exceptionOrNull()?.javaClass?.simpleName).also { lastResult = it }
    }

    /** 실패의 문구는 서버 로그에만(헬스에는 종류 이름만). 알림 서버의 주소 · 비밀값은 보내는 쪽이 문구에 싣지 않는다 */
    private fun logFailure(stage: String, error: Throwable) =
        log.error("observe:observe:threshold {} failed: {}: {}", stage, error.javaClass.simpleName, error.message?.take(200))

    /** 보낼 표의 건수(헬스용) — 읽지 못하면 null */
    fun alertCounts(): AlertCounts? = runCatching { queue.alertCounts() }.getOrNull()
}
