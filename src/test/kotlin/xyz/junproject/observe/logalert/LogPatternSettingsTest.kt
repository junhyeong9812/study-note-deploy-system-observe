package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.threshold.AlertSeverity

/**
 * ⑪-6a 글자 규칙의 설정 `LOG_ALERT_PATTERNS`: 규칙은 `;` 로 가르고(앞뒤 빈칸 · 빈 규칙은 무시) 하나는 `<이름>|<서비스>|<세기>|<글자>` —
 * 글자는 셋째 `|` 뒤의 전부(`|` · `=` · `,` · 안의 빈칸을 품는다 · 앞뒤 빈칸은 깎는다). 이름 `^[a-z0-9_]{1,40}$` · 서비스는 로그 줄의 서비스 꼴 ·
 * 세기 `info|warning|critical` · 글자 1~120자(숨은 문자 없음) · 규칙 20개까지 · 이름 중복 불가(그 이름의 규칙은 모두 버린다 — 무수신 설정과 같은 관례).
 * 틀린 규칙은 그 규칙만 버리고 까닭 낱말 하나를 남긴다. 비면 꺼짐.
 */
class LogPatternSettingsTest {
    @Test
    fun `규칙을 세미콜론으로 가르고 글자는 셋째 세로줄 뒤의 전부이며 앞뒤 빈칸만 깎는다`() {
        val settings = LogPatternSettings.parse(
            " claude_limit|backend|critical|claude run: result=limit_reached ; claude_auto | backend | warning |  result=answered auto=true  ;;" +
                "odd_text|backend|info|a|b=c,d e")
        assertEquals(listOf(
            LogPattern("claude_limit", "backend", AlertSeverity.CRITICAL, "claude run: result=limit_reached"),
            LogPattern("claude_auto", "backend", AlertSeverity.WARNING, "result=answered auto=true"),
            LogPattern("odd_text", "backend", AlertSeverity.INFO, "a|b=c,d e"),
        ), settings.patterns)
        assertEquals(emptyList<SettingsProblem>(), settings.problems)
        assertTrue(settings.enabled)
    }

    @Test
    fun `비거나 빈칸 · 세미콜론뿐이면 꺼짐이고 기동 로그에 꺼짐 한 줄`() {
        for (text in listOf("", "   ", " ; ;")) {
            val settings = LogPatternSettings.parse(text)
            assertFalse(settings.enabled, "'$text'")
            assertEquals(emptyList<SettingsProblem>(), settings.problems)
            assertEquals(listOf("log pattern alerts disabled — no patterns"), settings.startupLines())
        }
    }

    @Test
    fun `틀린 규칙은 그 규칙만 까닭 낱말과 함께 버리고 다른 규칙은 산다`() {
        val good = "ok|backend|warning|result=unavailable"
        val cases = listOf(
            "missing|backend|warning" to "fields_missing",
            "Bad-Name|backend|warning|x" to "name_invalid",
            "${"a".repeat(41)}|backend|warning|x" to "name_invalid",
            "|backend|warning|x" to "name_invalid",
            "svc|Backend Server|warning|x" to "service_invalid",
            "svc_empty||warning|x" to "service_invalid",
            "sev|backend|error|x" to "severity_invalid",
            "sev_case|backend|Warning|x" to "severity_invalid",
            "empty_text|backend|warning|   " to "text_empty",
            "long_text|backend|warning|${"x".repeat(121)}" to "text_too_long",
            "hidden|backend|warning|abc​def" to "text_hidden_characters",
        )
        for ((rule, reason) in cases) {
            val settings = LogPatternSettings.parse("$good;$rule")
            assertEquals(listOf("ok"), settings.patterns.map { it.name }, "$rule — 다른 규칙까지 버렸다")
            assertEquals(listOf(reason), settings.problems.map { it.reason }, rule)
        }
        // 경계의 안쪽 — 이름 40자 · 글자 120자는 받는다
        val inside = LogPatternSettings.parse("${"a".repeat(40)}|backend|info|${"x".repeat(120)}")
        assertEquals(1, inside.patterns.size)
        assertEquals(120, inside.patterns.single().text.length)
    }

    @Test
    fun `이름이 겹치면 그 이름의 규칙은 모두 버리고 까닭은 name_duplicated`() {
        val settings = LogPatternSettings.parse("dup|backend|info|a;dup|front|warning|b;other|backend|info|c")
        assertEquals(listOf("other"), settings.patterns.map { it.name })
        assertEquals(listOf("name_duplicated", "name_duplicated"), settings.problems.map { it.reason })
    }

    @Test
    fun `맞는 규칙은 20개까지이고 21번째부터는 too_many_patterns 로 버린다`() {
        val text = (1..22).joinToString(";") { "rule_$it|backend|info|text $it" }
        val settings = LogPatternSettings.parse(text)
        assertEquals((1..20).map { "rule_$it" }, settings.patterns.map { it.name })
        assertEquals(listOf("too_many_patterns", "too_many_patterns"), settings.problems.map { it.reason })
        // 틀린 규칙은 20개에 세지 않는다
        val withBroken = LogPatternSettings.parse("broken|backend|nope|x;" + (1..20).joinToString(";") { "rule_$it|backend|info|text $it" })
        assertEquals(20, withBroken.patterns.size)
        assertEquals(listOf("severity_invalid"), withBroken.problems.map { it.reason })
    }

    @Test
    fun `기동 로그에 쓰는 규칙과 버린 규칙이 남고 숨은 문자는 물음표로 바뀐다`() {
        val lines = LogPatternSettings.parse("claude_limit|backend|critical|result=limit_reached;bad\u0007|backend|info|x").startupLines()
        assertEquals(listOf("log pattern alerts watching claude_limit on backend (critical)", "log pattern alerts not using bad?|backend|info|x: name_invalid"), lines)
    }
}
