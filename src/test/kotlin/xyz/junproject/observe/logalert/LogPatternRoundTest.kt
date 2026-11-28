package xyz.junproject.observe.logalert

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.RoundOutcome
import java.time.Duration
import java.time.Instant

/**
 * ⑪-6a 글자 규칙 한 바퀴와 무수신 판정의 함께 살기 — 글자 규칙은 무수신 판정([LogAlertRound.judgeOnce])의 바퀴에서 **따로** 돈다:
 * 무수신이 꺼져도 · 실패해도 글자 규칙은 돌고, 글자 규칙이 실패해도 무수신은 돈다(예외는 새지 않고 ERROR 한 줄 — 종류 이름만).
 * 판정의 문(무수신과 같은 [LogJudgingGate] — 뜬 직후 · 수집이 밀린 동안)이 닫혀 있으면 글자 규칙도 판정하지 않는다.
 * 헬스 `log_alerts` 에 `pattern_rules`(쓰는 규칙 수) · `pattern_rules_dropped`(버린 규칙 수) · `pattern_last_judged_at` · `pattern_last_failure` 를 더한다(기존 칸은 그대로).
 */
class LogPatternRoundTest {
    private val now = Instant.parse("2026-11-29T03:00:00Z")
    private val pattern = LogPattern("claude_limit", "backend", AlertSeverity.CRITICAL, "result=limit_reached")
    private val patternSettings = LogPatternSettings(listOf(pattern), listOf(SettingsProblem("bad|x", "fields_missing")))
    private val silentSettings = LogAlertSettings(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30))), null, emptyList())
    private val logs = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }

    @AfterEach fun detach() { (LoggerFactory.getLogger("observe") as Logger).detachAppender(logs) }

    private class PatternStore : LogPatternStateStore {
        var failure: RuntimeException? = null
        var calls = 0
        var states: List<LogPatternState> = emptyList()
        val events = mutableListOf<ResourceAlert>()
        override fun judgePatternRound(now: Instant, judge: (previous: List<LogPatternState>) -> LogPatternJudgeResult): RoundOutcome {
            calls++
            failure?.let { throw it }
            val result = judge(states)
            states = result.states
            events += result.events
            return RoundOutcome(true, result.events.size)
        }
    }

    private class SilentStore : LogRuleStateStore {
        var failure: RuntimeException? = null
        var calls = 0
        override fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome {
            calls++
            failure?.let { throw it }
            return RoundOutcome(true, judge(emptyList()).events.size)
        }
    }

    private class Receipts : LogReceipts {
        override fun lastReceived(services: Collection<String>): Map<String, Instant?> = services.associateWith { null }
        override fun lastReceivedAny(): Instant? = null
        override fun silentServers(): Set<String> = emptySet()
    }

    private val matching = LogPatternLines { _, _, after, _ -> LogPatternMatches(1, "req-1", after.plusSeconds(1)) }
    private val patternStore = PatternStore()
    private val silentStore = SilentStore()
    private var judging = true

    private fun patternRound(settings: LogPatternSettings = patternSettings, lines: LogPatternLines = matching) =
        LogPatternRound(settings, patternStore, lines, LogPatternJudge(settings)) { judging }

    private fun round(silent: LogAlertSettings = silentSettings, patterns: LogPatternRound? = patternRound()) =
        LogAlertRound(silent, silentStore, Receipts(), LogSilentJudge(silent), patterns) { judging }

    @Test
    fun `무수신과 글자 규칙이 한 바퀴에 따로 판정되고 글자 규칙의 사건이 보낼 표로 간다`() {
        round().judgeOnce(now)
        assertEquals(1, silentStore.calls)
        assertEquals(1, patternStore.calls)
        assertEquals(listOf("log_pattern:claude_limit"), patternStore.events.map { it.key })
    }

    @Test
    fun `무수신이 꺼지고 글자 규칙만 켜져도 글자 규칙은 돈다 · 그 반대도`() {
        round(silent = LogAlertSettings(emptyList(), null, emptyList())).judgeOnce(now)
        assertEquals(0, silentStore.calls)
        assertEquals(1, patternStore.calls, "무수신이 꺼지자 글자 규칙이 돌지 않았다")
        round(patterns = patternRound(settings = LogPatternSettings(emptyList(), emptyList()))).judgeOnce(now)
        assertEquals(1, silentStore.calls, "글자 규칙이 꺼지자 무수신이 돌지 않았다")
        assertEquals(1, patternStore.calls, "꺼진 글자 규칙이 저장소에 닿았다")
    }

    @Test
    fun `한쪽이 실패해도 다른 쪽은 돌고 예외는 새지 않으며 ERROR 줄에는 종류 이름만 있다`() {
        patternStore.failure = IllegalStateException("db down result=limit_reached req-secret-path")
        val round = round()
        round.judgeOnce(now)
        assertEquals(1, silentStore.calls, "글자 규칙의 실패가 무수신을 막았다")
        assertEquals("IllegalStateException", round.health(now)["pattern_last_failure"])
        assertTrue(logs.list.any { it.formattedMessage == "observe:observe:log pattern round failed: IllegalStateException" }, "${logs.list.map { it.formattedMessage }}")
        assertFalse(logs.list.any { it.formattedMessage.contains("req-secret-path") }, "예외의 문구가 로그에 실렸다")

        patternStore.failure = null
        silentStore.failure = IllegalStateException("db down")
        val second = round()
        second.judgeOnce(now)
        assertEquals(1, patternStore.events.size, "무수신의 실패가 글자 규칙을 막았다")
    }

    @Test
    fun `판정의 문이 닫혀 있으면 글자 규칙도 판정하지 않는다`() {
        judging = false
        round().judgeOnce(now)
        assertEquals(0, patternStore.calls)
        assertEquals(emptyList<ResourceAlert>(), patternStore.events)
    }

    @Test
    fun `헬스 log_alerts 에 글자 규칙의 칸이 더해지고 기존 칸은 그대로다`() {
        val round = round()
        round.judgeOnce(now)
        val health = round.health(now)
        assertEquals(1, health["pattern_rules"])
        assertEquals(1, health["pattern_rules_dropped"])
        assertEquals(now.toString(), health["pattern_last_judged_at"])
        assertEquals("", health["pattern_last_failure"])
        assertEquals(setOf("enabled", "watched_services", "watch_all", "judging", "last_judged_at", "last_failure",
            "pattern_rules", "pattern_rules_dropped", "pattern_last_judged_at", "pattern_last_failure"), health.keys)
        assertEquals(true, health["enabled"], "무수신의 칸이 바뀌었다")
        assertEquals(1, health["watched_services"])
    }
}
