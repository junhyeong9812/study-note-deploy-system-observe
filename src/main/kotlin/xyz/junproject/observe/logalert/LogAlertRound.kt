package xyz.junproject.observe.logalert

import xyz.junproject.observe.threshold.SilentGate
import java.time.Duration
import java.time.Instant

/** 로그 무수신 판정 한 바퀴의 결과 — [failure] 는 실패한 예외의 종류 이름 */
data class LogAlertRoundResult(val ranAt: Instant, val judged: Boolean, val eventsCreated: Int, val failure: String?)

/**
 * 무수신 판정의 보류 — 이 프로세스가 뜬 직후 · 로그 수집이 따라잡기 전([warmUp] — 자원 끊김과 같은 [SilentGate]) ·
 * 로그 수집이 실패하거나 멈춘 동안([stallAfter])에는 판정하지 않는다. 뼈대(03): 아직 없다
 */
class LogJudgingGate(
    private val warmUp: SilentGate,
    private val collectorFailure: () -> String?,
    private val lastLoopAt: () -> Instant?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
) {
    fun active(now: Instant): Boolean = TODO("03: 판정의 보류")
}

/** 판정 한 바퀴 · 헬스 칸(`log_alerts`). 뼈대(03): 아직 없다 */
class LogAlertRound(
    private val settings: LogAlertSettings,
    private val store: LogRuleStateStore,
    private val receipts: LogReceipts,
    private val judge: LogSilentJudge,
    private val judging: (Instant) -> Boolean,
) {
    @Volatile var lastResult: LogAlertRoundResult? = null; private set
    @Volatile var lastJudgedAt: Instant? = null; private set

    fun judgeOnce(now: Instant): LogAlertRoundResult = TODO("03: 판정 한 바퀴")

    fun health(now: Instant): Map<String, Any?> = TODO("03: 헬스 칸")
}
