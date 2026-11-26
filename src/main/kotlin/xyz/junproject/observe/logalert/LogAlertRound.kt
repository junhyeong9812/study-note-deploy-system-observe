package xyz.junproject.observe.logalert

import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/** 로그 무수신 판정 한 바퀴의 결과 — [failure] 는 실패한 예외의 종류 이름 */
data class LogAlertRoundResult(val ranAt: Instant, val judged: Boolean, val eventsCreated: Int, val failure: String?)

/**
 * 무수신 판정의 문 — 이 프로세스가 뜬 지([startedAt]) [warmUp] 이 지나고(같으면 지난 것) · 로그 수집이 실패 중이 아니고 · 마지막 바퀴가
 * [stallAfter] 안이고(같으면 넘은 것) · 마지막으로 성공한 수집 바퀴가 따라잡은 바퀴([lastRoundCaughtUp] — 읽은 수가 한 바퀴의 상한보다 적었다)일 때만 판정한다.
 * 수집이 다시 밀린 동안의 공백은 로그가 없는 것이 아니다 — 따라잡지 못한 채 오래 지나도 판정하지 않는다(자원 끊김의 `SilentGate` 와 달리
 * 포기하지 않는다. 밀린 동안의 판정은 거짓 알림이다 — 영영 밀리면 헬스의 `log_alerts.judging` 이 false 로 보인다).
 */
class LogJudgingGate(
    private val startedAt: Instant,
    private val warmUp: Duration,
    private val collectorFailure: () -> String?,
    private val lastLoopAt: () -> Instant?,
    private val lastRoundCaughtUp: () -> Boolean,
    private val stallAfter: Duration = Duration.ofSeconds(60),
) {
    fun active(now: Instant): Boolean {
        if (Duration.between(startedAt, now) < warmUp || collectorFailure() != null) return false
        val loop = lastLoopAt() ?: return false
        return Duration.between(loop, now) < stallAfter && lastRoundCaughtUp()
    }
}

/**
 * 로그 무수신 판정 한 바퀴 · 헬스 칸(`log_alerts`). 설정이 꺼져 있으면 아무것도 읽지 않는다.
 * 끊긴 서버는 잠금 트랜잭션 밖에서 먼저 읽고, 마지막 로그는 잠금을 얻은 바퀴 안에서 읽는다. 어떤 실패도 예외로 새지 않는다 —
 * 결과 · 헬스에 종류 이름, ERROR 한 줄(종류 이름만 — 예외의 문구에는 주소 · 로그의 글이 실릴 수 있다). [judging] 은 보류([LogJudgingGate]).
 */
class LogAlertRound(
    private val settings: LogAlertSettings,
    private val store: LogRuleStateStore,
    private val receipts: LogReceipts,
    private val judge: LogSilentJudge,
    /**
     * 뼈대(⑪-6a): 글자 규칙 — 있으면 [judgeOnce] 가 무수신 판정과 **따로** 돌리고(무수신이 꺼져도 · 실패해도 돌고, 글자 규칙의 실패가 무수신을 막지 않는다)
     * [health] 에 [LogPatternRound.health] 의 칸을 더한다(없으면 기존 칸 그대로). 지금은 쓰지 않는다 — 구현 워커가 잇는다
     */
    @Suppress("unused") private val patterns: LogPatternRound? = null,
    private val judging: (Instant) -> Boolean,
) {
    @Volatile var lastResult: LogAlertRoundResult? = null; private set
    @Volatile var lastJudgedAt: Instant? = null; private set

    private val log = LoggerFactory.getLogger("observe")

    fun judgeOnce(now: Instant): LogAlertRoundResult {
        if (!settings.enabled) return LogAlertRoundResult(now, false, 0, null).also { lastResult = it }
        val result = try {
            val judgingNow = judging(now)
            val silentServers = if (settings.services.isEmpty()) emptySet() else receipts.silentServers()
            val outcome = store.judgeRound(now) { previous ->
                val lastReceived = receipts.lastReceived(settings.services.map { it.service })
                val lastReceivedAny = if (settings.allThreshold == null) null else receipts.lastReceivedAny()
                judge.judge(LogJudgeInput(now, lastReceived, lastReceivedAny, judgingNow, silentServers), previous)
            }
            if (outcome.judged) lastJudgedAt = now
            if (outcome.eventsCreated > 0) log.info("observe:observe:log alert round created {} alerts", outcome.eventsCreated)
            LogAlertRoundResult(now, outcome.judged, outcome.eventsCreated, null)
        } catch (error: Exception) {
            log.error("observe:observe:log alert round failed: {}", error.javaClass.simpleName)
            LogAlertRoundResult(now, false, 0, error.javaClass.simpleName)
        }
        return result.also { lastResult = it }
    }

    /** `log_alerts` 칸 — `{enabled, watched_services, watch_all, judging, last_judged_at, last_failure}`(시각 · 실패가 없으면 빈 글자). DB 를 읽지 않는다 */
    fun health(now: Instant): Map<String, Any?> = mapOf(
        "enabled" to settings.enabled,
        "watched_services" to settings.services.size,
        "watch_all" to (settings.allThreshold != null),
        "judging" to runCatching { judging(now) }.getOrDefault(false),
        "last_judged_at" to (lastJudgedAt?.toString() ?: ""),
        "last_failure" to (lastResult?.failure ?: ""),
    )
}
