package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * 보낼 표에 드는 알림의 규칙은 한 타입 체계([AlertRule]) — 자원 규칙([ResourceRule]) 여섯과 로그 규칙([LogRule]) 둘(`log_silent` 서비스별 ·
 * `log_silent_all` 전체 한 줄, kind 는 둘 다 `log_silent`). 규칙 글자 ↔ 규칙 · 등급 글자 ↔ 등급의 변환은 글자가 정확히 같아야 하고 모르는 글자에 예외를 낸다.
 * 보내기의 대상 묶음: 디스크 두 단계만 한 묶음(`disk`), 로그 규칙은 규칙 글자 그대로.
 */
class AlertRuleTest {
    @Test
    fun `이 판이 아는 규칙은 자원 규칙 여섯과 로그 규칙 둘이고 규칙 글자는 서로 다르다`() {
        assertEquals(ResourceRule.entries + LogRule.entries, AlertRule.entries)
        assertEquals(listOf("disk_warning", "disk_critical", "memory", "cpu", "restarts", "silent", "log_silent", "log_silent_all"),
            AlertRule.entries.map { it.wireName })
        assertEquals(AlertRule.entries.size, AlertRule.entries.map { it.wireName }.toSet().size, "규칙 글자가 겹친다")
    }

    @Test
    fun `로그 규칙의 kind 는 둘 다 log_silent 이고 알림 서버가 받는 꼴이다`() {
        assertEquals(listOf("log_silent", "log_silent"), LogRule.entries.map { it.kind })
        for (rule in AlertRule.entries) assertTrue(rule.kind.matches(Regex("^[a-z0-9_]{1,40}$")), "알림 서버가 받지 않는 kind: ${rule.kind}")
    }

    @Test
    fun `규칙 글자에서 규칙으로 바꾸면 아는 규칙은 모두 제자리로 돌아온다`() {
        for (rule in AlertRule.entries) assertSame(rule, AlertRule.fromWireName(rule.wireName), rule.wireName)
    }

    @Test
    fun `규칙 글자가 정확히 같지 않으면 규칙으로 바꾸지 않고 예외를 낸다`() {
        for (unknown in listOf("", "log_errors", "LOG_SILENT", "Memory", "log_silent ", " log_silent", "log_silent_All", "disk", "log", "resource_memory")) {
            assertThrows<IllegalStateException>("'$unknown' 을 규칙으로 바꿨다") { AlertRule.fromWireName(unknown) }
        }
    }

    @Test
    fun `등급 글자는 info · warning · critical 셋만 등급으로 바뀌고 그 밖은 예외를 낸다`() {
        assertEquals(listOf(AlertSeverity.INFO, AlertSeverity.WARNING, AlertSeverity.CRITICAL),
            listOf("info", "warning", "critical").map { AlertSeverity.fromWireName(it) })
        for (unknown in listOf("", "error", "Warning", "WARNING", "warning ", "fatal")) {
            assertThrows<IllegalStateException>("'$unknown' 을 등급으로 바꿨다") { AlertSeverity.fromWireName(unknown) }
        }
    }

    @Test
    fun `보내기의 대상 묶음은 디스크 두 단계만 하나이고 로그 규칙은 규칙 글자 그대로다`() {
        assertEquals("disk", AlertTarget.groupOf(ResourceRule.DISK_WARNING))
        assertEquals("disk", AlertTarget.groupOf(ResourceRule.DISK_CRITICAL))
        assertEquals("log_silent", AlertTarget.groupOf(LogRule.SILENT))
        assertEquals("log_silent_all", AlertTarget.groupOf(LogRule.SILENT_ALL))
        assertEquals("memory", AlertTarget.groupOf(ResourceRule.MEMORY))
    }
}
