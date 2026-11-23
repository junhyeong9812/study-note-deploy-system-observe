package xyz.junproject.observe.threshold

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

    fun runOnce(now: Instant): ThresholdRoundResult = TODO("not implemented")

    /** 보낼 표의 건수(헬스용) — 읽지 못하면 null */
    fun alertCounts(): AlertCounts? = TODO("not implemented")
}
