package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.logalert.LogPattern
import java.time.Instant

/**
 * ⑪-6a 글자 규칙의 사건 꼴 — `source=observe` · `kind=log_pattern` · `key=log_pattern:<이름>` · 세기 = 규칙의 세기 · title `로그 글자 규칙 <이름> — <서비스>` ·
 * detail `<n>줄 맞음 · 첫 줄 요청 ID <ID|없음> · 시각 <ISO|모름>`. 로그의 내용(message)도 규칙의 글자도 싣지 않는다 · 요청 ID 가 관측 서버가 받는 꼴
 * (`^[A-Za-z0-9_.-]{1,80}$`) 밖이면 `없음` · 알림 서버의 꼴 제한(kind `^[a-z0-9_]{1,40}$` · key 120자 · title 1~200자 · detail 1500자)을 지킨다.
 */
class LogPatternAlertTest {
    private val now = Instant.parse("2026-11-29T03:00:00Z")
    private val pattern = LogPattern("claude_limit", "backend", AlertSeverity.CRITICAL, "result=limit_reached")

    @Test
    fun `사건의 꼴은 정한 글자 그대로다`() {
        val alert = LogAlerts.pattern(pattern, 3, "req-3f9a0c1b2d4e", now.minusSeconds(40), now)
        assertEquals(mapOf("source" to "observe", "kind" to "log_pattern", "severity" to "critical", "key" to "log_pattern:claude_limit",
            "title" to "로그 글자 규칙 claude_limit — backend", "detail" to "3줄 맞음 · 첫 줄 요청 ID req-3f9a0c1b2d4e · 시각 ${now.minusSeconds(40)}"), alert.eventBody())
        assertEquals("log_pattern:claude_limit@${now.toEpochMilli()}", alert.eventId)
        assertEquals(LogPatternRule.PATTERN, alert.rule)
        assertEquals("claude_limit", alert.targetName)
        assertEquals(now, alert.createdAt)
    }

    @Test
    fun `요청 ID 가 받는 꼴 밖이면 싣지 않고 없음이다`() {
        for (requestId in listOf("req:1", "a".repeat(81), "req 1", "", null)) {
            val detail = LogAlerts.pattern(pattern, 1, requestId, now, now).detail
            assertEquals("1줄 맞음 · 첫 줄 요청 ID 없음 · 시각 $now", detail, "$requestId")
        }
        assertTrue(LogAlerts.pattern(pattern, 1, "a".repeat(80), now, now).detail.contains("a".repeat(80)))
    }

    @Test
    fun `규칙의 글자와 로그의 내용은 사건 어디에도 없다 · 가장 긴 이름에서도 알림 서버의 꼴 안이다`() {
        val longest = LogPattern("n".repeat(40), "a".repeat(40), AlertSeverity.INFO, "SECRET-TEXT-" + "x".repeat(108))
        val alert = LogAlerts.pattern(longest, Long.MAX_VALUE, "r".repeat(80), now, now)
        val body = alert.eventBody()
        assertFalse(body.values.any { it.contains("SECRET-TEXT") }, "규칙의 글자를 실었다: $body")
        assertTrue(alert.kind.matches(Regex("^[a-z0-9_]{1,40}$")))
        assertTrue(alert.key.length <= 120, alert.key)
        assertTrue(alert.title.length in 1..200, alert.title)
        assertTrue(alert.detail.length <= 1500)
    }

    @Test
    fun `글자 규칙은 이 판이 아는 규칙이다 — 보내기가 모르는 규칙으로 건너뛰지 않는다`() {
        assertSame(LogPatternRule.PATTERN, AlertRule.fromWireName("log_pattern"))
        assertTrue(LogPatternRule.PATTERN in AlertRule.entries)
    }
}
