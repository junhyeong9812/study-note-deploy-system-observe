package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * 로그 무수신 감시의 설정(`LOG_ALERT_SERVICES` · `LOG_ALERT_ALL_THRESHOLD_MINUTES`): 한 줄 = `<서비스>@<사는 서버>=<기준(분)>`, 줄은 쉼표로(앞뒤 공백 · 빈 줄은 무시).
 * 기준은 1~360분(6시간까지) — 없음 · 숫자가 아님 · 0 이하 · 360 초과면 그 서비스는 감시하지 않는다. 서비스 이름이 `*` · 빈 글자 · 숨은 문자(제어 ·
 * U+2028/2029 · 서식 문자) · 짝 없는 서로게이트를 담거나 두 번 나오면(서버가 달라도 — 그 이름의 줄 모두) 그 줄만 거절한다. 서버가 없거나 같은 꼴로 이상해도 그 줄만 거절.
 * 받아들이지 않은 줄마다 까닭의 낱말 하나: `name_reserved` · `name_empty` · `name_hidden_characters` · `name_duplicated` · `server_missing` · `server_invalid` ·
 * `no_threshold` · `threshold_not_number` · `threshold_not_positive` · `threshold_over_6h` (이름 → 서버 → 기준의 차례로 첫 까닭).
 * §16-3: 이름 · 서버 · 기준의 조각은 앞뒤 공백을 다듬는다. 이름이 수집이 받는 꼴(`[a-z0-9-]{1,40}`)이 아니면 `name_invalid` — 이름의 까닭 차례는
 * `name_reserved` → `name_empty` → `name_hidden_characters` → `name_invalid` → `name_duplicated`. 사는 서버가 기대 서버 목록에 없으면 받아들이되
 * [LogAlertSettings.warnings] 에 (`<서비스>@<서버>`, `server_not_expected`) 한 줄 · 기동 로그에도 — 목록이 비어 있으면 대조하지 않는다.
 * 전체 한 줄의 기준도 같은 규칙(비면 그 줄은 감시하지 않는다 — 까닭 없음). 기동 로그의 줄([LogAlertSettings.startupLines])에 감시하는 것 · 받아들이지 않은 것(다듬은 글자와 까닭) ·
 * 아무것도 감시하지 않으면 `disabled` 가 드러난다.
 */
class LogAlertSettingsTest {
    private fun parse(services: String, all: String = "") = LogAlertSettings.parse(services, all)

    private val hidden = listOf('\n', '\r', '\t', '\u0000', '\u001b', '\u007f', '\u0085', ' ', ' ', '​', '‮', '﻿')

    private fun assertPrintable(text: String) {
        for (character in hidden) assertFalse(text.contains(character), "U+%04X 가 남았다: %s".format(character.code, text))
        text.forEachIndexed { index, character ->
            if (Character.isHighSurrogate(character)) assertTrue(index + 1 < text.length && Character.isLowSurrogate(text[index + 1]), "짝 없는 서로게이트: $text")
            if (Character.isLowSurrogate(character)) assertTrue(index > 0 && Character.isHighSurrogate(text[index - 1]), "짝 없는 서로게이트: $text")
        }
    }

    @Test
    fun `서비스 · 사는 서버 · 기준 분의 줄을 쉼표로 읽고 앞뒤 공백과 빈 줄은 무시한다`() {
        val settings = parse(" backend@ci-agent-158=30 ,front@ci-agent-9=45,, ", "60")
        assertEquals(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30)), WatchedService("front", "ci-agent-9", Duration.ofMinutes(45))),
            settings.services)
        assertEquals(Duration.ofMinutes(60), settings.allThreshold)
        assertEquals(emptyList<SettingsProblem>(), settings.problems)
        assertTrue(settings.enabled)
    }

    @Test
    fun `기준이 없거나 숫자가 아니거나 0 이하이거나 6시간을 넘으면 그 서비스만 감시하지 않고 까닭이 남는다 — 360분은 감시한다`() {
        val cases = mapOf(
            "a@s" to "no_threshold", "b@s=" to "no_threshold", "c@s=abc" to "threshold_not_number", "d@s=1.5" to "threshold_not_number",
            "e@s=0" to "threshold_not_positive", "f@s=-5" to "threshold_not_positive", "g@s=361" to "threshold_over_6h",
        )
        val settings = parse((cases.keys + "ok@s=360" + "one@s=1").joinToString(","))
        assertEquals(listOf(WatchedService("ok", "s", Duration.ofMinutes(360)), WatchedService("one", "s", Duration.ofMinutes(1))), settings.services)
        assertEquals(cases.map { (entry, reason) -> SettingsProblem(entry, reason) }, settings.problems)
    }

    @Test
    fun `서비스 이름이 별표 · 빈 글자 · 숨은 문자 · 짝 없는 서로게이트면 그 줄만 거절하고 다른 서비스의 감시는 계속한다`() {
        val names = mapOf(
            "*" to "name_reserved", "" to "name_empty", "a\nb" to "name_hidden_characters", "a\rb" to "name_hidden_characters",
            "a\u0000b" to "name_hidden_characters", "a b" to "name_hidden_characters", "a b" to "name_hidden_characters",
            "a​b" to "name_hidden_characters", "a‮b" to "name_hidden_characters", "a﻿b" to "name_hidden_characters",
            "a\uD800b" to "name_hidden_characters", "a\uDC00" to "name_hidden_characters",
        )
        val settings = parse((names.keys.map { "$it@ci-agent-158=30" } + "backend@ci-agent-158=30").joinToString(","))
        assertEquals(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30))), settings.services, "거절할 이름을 받아들였거나 다른 서비스를 멈췄다")
        assertEquals(names.values.toList(), settings.problems.map { it.reason })
        for (problem in settings.problems) assertPrintable(problem.entry)
        assertEquals("a?b@ci-agent-158=30", settings.problems[2].entry, "받아들이지 않은 줄은 다듬은 글자로 남는다")
    }

    @Test
    fun `같은 서비스 이름이 두 번 나오면 서버가 달라도 그 이름의 줄을 모두 거절하고 다른 서비스는 감시한다`() {
        val settings = parse("backend@ci-agent-158=30,front@ci-agent-9=30,backend@ci-agent-164=45")
        assertEquals(listOf(WatchedService("front", "ci-agent-9", Duration.ofMinutes(30))), settings.services)
        assertEquals(listOf(SettingsProblem("backend@ci-agent-158=30", "name_duplicated"), SettingsProblem("backend@ci-agent-164=45", "name_duplicated")),
            settings.problems)
    }

    @Test
    fun `사는 서버가 없거나 이상하면 그 줄만 거절한다`() {
        val settings = parse("a=30,b@=30,c@*=30,d@ci\nagent=30,ok@ci-agent-9=30")
        assertEquals(listOf(WatchedService("ok", "ci-agent-9", Duration.ofMinutes(30))), settings.services)
        assertEquals(listOf("server_missing", "server_missing", "server_invalid", "server_invalid"), settings.problems.map { it.reason })
    }

    @Test
    fun `전체 한 줄의 기준은 비면 감시하지 않고 이상하면 감시하지 않으며 까닭이 남는다`() {
        assertNull(parse("backend@s=30", "").allThreshold)
        assertEquals(emptyList<SettingsProblem>(), parse("backend@s=30", " ").problems)
        for ((text, reason) in mapOf("abc" to "threshold_not_number", "0" to "threshold_not_positive", "361" to "threshold_over_6h")) {
            val settings = parse("backend@s=30", text)
            assertNull(settings.allThreshold, text)
            assertEquals(listOf(SettingsProblem("log_silent_all=$text", reason)), settings.problems)
        }
        assertEquals(Duration.ofMinutes(360), parse("", "360").allThreshold)
        assertTrue(parse("", "30").enabled, "전체 한 줄만 감시해도 판정은 켜진다")
    }

    @Test
    fun `설정이 통째로 비거나 받아들인 줄이 없으면 판정이 꺼지고 기동 로그에 disabled 가 드러난다`() {
        for (settings in listOf(parse("", ""), parse("*@s=30,x@s=abc", "abc"))) {
            assertFalse(settings.enabled)
            val lines = settings.startupLines()
            assertTrue(lines.any { it.contains("disabled") }, "판정이 꺼진 것이 기동 로그에 없다: $lines")
        }
    }

    @Test
    fun `기동 로그에는 감시하는 서비스와 기준 · 받아들이지 않은 줄의 다듬은 글자와 까닭이 한 줄씩 드러난다`() {
        val settings = parse("backend@ci-agent-158=30,a​b@s=30,front@ci-agent-9=400", "45")
        val lines = settings.startupLines()
        for (line in lines) assertPrintable(line)
        assertTrue(lines.any { it.contains("backend@ci-agent-158") && it.contains("30") }, "감시하는 서비스가 드러나지 않았다: $lines")
        assertTrue(lines.any { it.contains("a?b@s=30") && it.contains("name_hidden_characters") }, "거절한 줄이 드러나지 않았다: $lines")
        assertTrue(lines.any { it.contains("front@ci-agent-9=400") && it.contains("threshold_over_6h") }, "감시하지 않는 줄이 드러나지 않았다: $lines")
        assertTrue(lines.any { it.contains("log_silent_all") && it.contains("45") }, "전체 한 줄의 기준이 드러나지 않았다: $lines")
        assertFalse(lines.any { it.contains("disabled") })
    }

    // ── §16-3: 다듬기 · 수집이 받는 꼴 · 기대 서버 ─────────

    @Test
    fun `이름 · 서버 · 기준의 조각은 앞뒤 공백을 다듬어 읽는다`() {
        val settings = parse("backend @ ci-agent-158 = 30 ,  front@ ci-agent-9 =45")
        assertEquals(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30)), WatchedService("front", "ci-agent-9", Duration.ofMinutes(45))),
            settings.services)
        assertEquals(emptyList<SettingsProblem>(), settings.problems)
        assertEquals(listOf(SettingsProblem("backend@s=30", "name_duplicated"), SettingsProblem("backend @ t = 30", "name_duplicated")),
            parse("backend@s=30, backend @ t = 30").problems, "다듬은 이름으로 중복을 보지 않았다")
    }

    @Test
    fun `이름이 수집이 받는 꼴이 아니면 name_invalid 로 그 줄만 거절하고 40자까지는 받는다`() {
        val invalid = listOf("Backend", "back end", "a".repeat(41), "back_end", "백엔드", "back.end")
        val settings = parse((invalid.map { "$it@ci-agent-158=30" } + "${"a".repeat(40)}@ci-agent-158=30" + "ci-cd-2@ci-agent-9=30").joinToString(","))
        assertEquals(listOf("a".repeat(40), "ci-cd-2"), settings.services.map { it.service })
        assertEquals(invalid.map { "name_invalid" }, settings.problems.map { it.reason })
    }

    @Test
    fun `이름의 까닭은 예약 · 빈 글자 · 숨은 문자 · 꼴 · 중복의 차례로 하나다`() {
        val settings = parse("*@s=30, @s=30, A\nB@s=30, Backend@s=30, Backend@t=30, ok@s=30, ok@t=30")
        assertEquals(listOf("name_reserved", "name_empty", "name_hidden_characters", "name_invalid", "name_invalid", "name_duplicated", "name_duplicated"),
            settings.problems.map { it.reason })
    }

    @Test
    fun `사는 서버가 기대 서버 목록에 없으면 받아들이되 경고 한 줄이 남고 목록이 비면 대조하지 않는다`() {
        val expected = setOf("ci-agent-158", "ci-agent-164")
        val settings = LogAlertSettings.parse("backend@ci-agent-158=30,front@ci-agent-9=45", "", expected)
        assertEquals(listOf("backend", "front"), settings.services.map { it.service }, "기대 서버 목록에 없는 서버의 서비스를 거절했다")
        assertEquals(emptyList<SettingsProblem>(), settings.problems)
        assertEquals(listOf(SettingsProblem("front@ci-agent-9", "server_not_expected")), settings.warnings)
        val lines = settings.startupLines()
        assertTrue(lines.any { it.contains("front@ci-agent-9") && it.contains("server_not_expected") }, "기동 로그에 경고가 없다: $lines")
        assertEquals(emptyList<SettingsProblem>(), LogAlertSettings.parse("backend@ci-agent-158=30,front@ci-agent-9=45", "", emptySet()).warnings,
            "기대 서버 목록이 비었는데 대조했다")
    }
}
