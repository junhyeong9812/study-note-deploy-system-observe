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
) {
    @Volatile var lastResult: ThresholdRoundResult? = null; private set
    @Volatile var lastJudgedAt: Instant? = null; private set

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
