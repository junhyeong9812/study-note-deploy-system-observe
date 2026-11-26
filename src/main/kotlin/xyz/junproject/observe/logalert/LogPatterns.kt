package xyz.junproject.observe.logalert

import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.RoundOutcome
import java.time.Duration
import java.time.Instant

/** 뼈대(⑪-6a) — 본문(TODO)은 구현 워커가 채운다. 로그 글자 규칙 하나 — 이 서비스의 로그 줄 내용(message)에 [text] 가 그대로 들어 있으면 맞는다 */
data class LogPattern(val name: String, val service: String, val severity: AlertSeverity, val text: String)

/** 글자 규칙의 설정(`LOG_ALERT_PATTERNS`) — [problems] 는 버린 규칙(다듬은 글자와 까닭) */
data class LogPatternSettings(val patterns: List<LogPattern>, val problems: List<SettingsProblem>) {
    val enabled: Boolean get() = patterns.isNotEmpty()

    /**
     * 뼈대: 기동 로그의 줄 — 쓰는 규칙마다 `log pattern alerts watching <이름> on <서비스> (<세기>)`, 버린 규칙마다 `log pattern alerts not using <다듬은 규칙>: <까닭>`,
     * 쓰는 규칙이 없으면 `log pattern alerts disabled — no patterns`. 본문은 구현 워커가 채운다
     */
    fun startupLines(): List<String> = TODO("뼈대: 구현 워커가 채운다")

    companion object {
        const val MAX_PATTERNS = 20
        const val MAX_TEXT_LENGTH = 120
        val NAME = Regex("^[a-z0-9_]{1,40}$")

        /**
         * 뼈대: `LOG_ALERT_PATTERNS` 를 읽는다 — 규칙은 `;` 로(앞뒤 빈칸 · 빈 규칙은 무시), 하나는 `<이름>|<서비스>|<세기>|<글자>`(셋째 `|` 뒤의 전부가 글자 — 조각마다 앞뒤 빈칸을 깎는다).
         * 까닭 낱말(규칙마다 첫 하나 — 이 차례로): `fields_missing`(조각이 넷이 안 됨) · `name_invalid`([NAME] 밖) · `name_duplicated`(같은 이름의 규칙은 모두) ·
         * `service_invalid`(로그 줄의 서비스 꼴 밖) · `severity_invalid`(`info|warning|critical` 밖 — 대소문자 구분) · `text_empty` · `text_too_long`([MAX_TEXT_LENGTH] 초과) ·
         * `text_hidden_characters`(숨은 문자) · `too_many_patterns`(맞는 규칙이 이미 [MAX_PATTERNS] 개). 버린 규칙의 글자는 숨은 문자를 '?' 로. 본문은 구현 워커가 채운다
         */
        fun parse(text: String): LogPatternSettings = TODO("뼈대: 구현 워커가 채운다")
    }
}
/** 한 규칙 · 한 기간에 맞은 줄 — 수와 첫 줄(스트림 차례로)의 요청 ID · 시각 */
data class LogPatternMatches(val count: Long, val firstRequestId: String?, val firstAt: Instant?)

/** 판정이 읽는 것 — 이 서비스의 줄 가운데 시각(`stream_ms`)이 `(after, until]` 이고 내용에 [text] 가 그대로 든 줄 */
fun interface LogPatternLines {
    fun matches(service: String, text: String, after: Instant, until: Instant): LogPatternMatches
}

/** 글자 규칙 하나의 상태 — 어디까지 봤는가 · 마지막으로 알린 때 · 아직 알리지 않은 맞춤 */
data class LogPatternState(
    val name: String,
    val checkedUntil: Instant,
    val lastNotifiedAt: Instant?,
    val pendingCount: Long,
    val pendingFirstRequestId: String?,
    val pendingFirstAt: Instant?,
)

data class LogPatternJudgeResult(val states: List<LogPatternState>, val events: List<ResourceAlert>)

/** 글자 규칙의 상태 저장 — 잠금 · 상태 · 보낼 알림을 한 트랜잭션에 */
interface LogPatternStateStore {
    fun judgePatternRound(now: Instant, judge: (previous: List<LogPatternState>) -> LogPatternJudgeResult): RoundOutcome
}

/**
 * 글자 규칙의 판정 — 규칙마다 따로: 이번에 볼 줄은 시각이 `(max(앞에서 본 곳, now − [freshness]), now − [settle]]`(받은 때보다 [freshness] 넘게 오래된 줄 —
 * 다시 뜬 뒤의 밀린 묶음 — 은 보지 않는다). 맞은 줄은 쌓아 두고, 마지막 알림에서 [window] 가 지났으면(같으면 지난 것 · 알린 적 없으면 바로) 쌓인 것을 한 건으로 알린다 —
 * 맞춤이 그친 뒤에도 창이 지나면 다음 판정이 쌓인 수를 낸다. 자기 설정에 없는 앞 상태는 그대로 돌려준다
 */
class LogPatternJudge(
    private val settings: LogPatternSettings,
    private val window: Duration = Duration.ofMinutes(10),
    private val freshness: Duration = Duration.ofMinutes(5),
    private val settle: Duration = Duration.ofSeconds(15),
) {
    /** 뼈대: 위 규칙의 판정 — 본문은 구현 워커가 채운다 */
    fun judge(now: Instant, previous: List<LogPatternState>, lines: LogPatternLines): LogPatternJudgeResult = TODO("뼈대: 구현 워커가 채운다")
}

/** 글자 규칙 판정 한 바퀴 — 예외를 내지 않는다(결과 · 헬스에 종류 이름, ERROR 한 줄). [judging] 이 false 면 판정하지 않는다(무수신 판정과 같은 문) */
class LogPatternRound(
    private val settings: LogPatternSettings,
    private val store: LogPatternStateStore,
    private val lines: LogPatternLines,
    private val judge: LogPatternJudge,
    private val judging: (Instant) -> Boolean,
) {
    /**
     * 뼈대: 한 바퀴 — 규칙이 없으면 아무것도 읽지 않는다 · [judging] 이 false 면 판정하지 않는다 · [LogPatternStateStore.judgePatternRound] 에 [LogPatternJudge.judge] 를 싣는다 ·
     * 실패는 예외로 새지 않고 ERROR 한 줄 `observe:observe:log pattern round failed: <종류>` · 만든 사건 수를 돌려준다. 본문은 구현 워커가 채운다
     */
    fun judgeOnce(now: Instant): Int = TODO("뼈대: 구현 워커가 채운다")

    /** 뼈대: 헬스 `log_alerts` 에 더할 칸 — `pattern_rules` · `pattern_rules_dropped` · `pattern_last_judged_at`(없으면 "") · `pattern_last_failure`(없으면 ""). 본문은 구현 워커가 채운다 */
    fun health(): Map<String, Any?> = TODO("뼈대: 구현 워커가 채운다")
}
