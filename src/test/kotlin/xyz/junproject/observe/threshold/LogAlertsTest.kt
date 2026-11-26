package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 로그 알림의 몸([LogAlerts]) — 알림 서버로 가는 꼴은 자원 알림과 같다(`{source:"observe", kind, severity, key, title, detail}`):
 * - 서비스별(`log_silent`): key `log:<서비스>:silent`(해소는 뒤에 `:resolved`), 서버 = 그 서비스가 사는 서버, 대상 이름 = 서비스.
 * - 전체 한 줄(`log_silent_all`): key `log:*:silent`, 서버 `*`, 대상 이름 없음.
 * - 등급: 넘음 warning · 해소 info. 고유 키 = `<key>@<판정 시각 epoch 밀리초>`(자원 알림과 같은 꼴), 만든 시각 = 판정 시각.
 * - 글에 드는 것은 서비스 이름 · 서버 이름 · 마지막으로 받은 시각 · 기준뿐이다(로그의 글은 받지도 싣지도 않는다).
 *   서비스 · 서버 이름의 제어 문자 · 줄 · 문단 구분 문자 · 서식 문자(Cf)는 `?` 로 — key · title · detail 어디에도 남지 않는다.
 * - 길이: key 120자 이하(긴 이름은 줄이되 같은 서비스는 늘 같은 key · 다른 서비스는 다른 key · 해소 key = 넘음 key + `:resolved`) ·
 *   title 1~200자 · 한 줄 · detail 1500자 이하.
 */
class LogAlertsTest {
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val lastLog = Instant.parse("2026-11-26T02:20:00Z")
    private val threshold = Duration.ofMinutes(30)

    private val hiddenCharacters = listOf('\n', '\r', '\t', '\u0000', '\u001b', '\u007f', '\u0085', ' ', ' ', '​', '‮', '﻿')

    private fun assertBodyShape(alert: ResourceAlert) {
        val body = alert.eventBody()
        assertEquals(listOf("source", "kind", "severity", "key", "title", "detail"), body.keys.toList())
        assertEquals("observe", body["source"])
        assertEquals("log_silent", body["kind"])
        assertTrue(alert.key.length <= 120, "key 가 120자를 넘는다(${alert.key.length})")
        assertTrue(alert.title.isNotBlank() && alert.title.length <= 200, "title 이 비었거나 200자를 넘는다(${alert.title.length})")
        assertTrue(alert.detail.length <= 1500, "detail 이 1500자를 넘는다(${alert.detail.length})")
        for (line in listOf('\n', '\r', ' ', ' ')) assertFalse(alert.title.contains(line), "title 이 한 줄이 아니다: ${alert.title}")
    }

    @Test
    fun `서비스별 무수신의 넘음은 log_silent 규칙 · warning · 서비스의 key 로 만들어진다`() {
        val alert = LogAlerts.silent("backend", "ci-agent-158", lastLog, threshold, now, resolved = false)
        assertBodyShape(alert)
        assertEquals(LogRule.SILENT, alert.rule)
        assertEquals(AlertSeverity.WARNING, alert.severity)
        assertEquals("log:backend:silent", alert.key)
        assertEquals("log:backend:silent@${now.toEpochMilli()}", alert.eventId)
        assertEquals("ci-agent-158", alert.server)
        assertEquals("backend", alert.targetName)
        assertEquals(now, alert.createdAt)
        assertEquals(AlertTarget("ci-agent-158", "log_silent", "backend"), AlertTarget.of(alert))
        assertTrue(alert.title.contains("backend"), "title 에 서비스가 없다: ${alert.title}")
        assertTrue(alert.detail.contains(lastLog.toString()), "detail 에 마지막으로 받은 시각이 없다: ${alert.detail}")
        assertTrue(alert.detail.contains("30분"), "detail 에 기준이 없다: ${alert.detail}")
    }

    @Test
    fun `서비스별 무수신의 해소는 info 이고 key 는 넘음 key 뒤에 resolved 가 붙으며 글이 넘음과 다르다`() {
        val firing = LogAlerts.silent("backend", "ci-agent-158", lastLog, threshold, now, resolved = false)
        val resolved = LogAlerts.silent("backend", "ci-agent-158", now.minusSeconds(5), threshold, now.plusSeconds(60), resolved = true)
        assertBodyShape(resolved)
        assertEquals(AlertSeverity.INFO, resolved.severity)
        assertEquals(firing.key + ":resolved", resolved.key)
        assertEquals("${firing.key}:resolved@${now.plusSeconds(60).toEpochMilli()}", resolved.eventId)
        assertNotEquals(firing.title, resolved.title, "넘음과 해소의 title 이 같다")
        assertEquals(AlertTarget.of(firing), AlertTarget.of(resolved), "넘음과 해소가 다른 대상이다(도착 순서가 바뀔 수 있다)")
    }

    @Test
    fun `전체 한 줄의 무수신은 log_silent_all 규칙 · 서버 별표 · 대상 이름 없음 · key log 별표 silent 다`() {
        val firing = LogAlerts.silentAll(lastLog, Duration.ofMinutes(45), now, resolved = false)
        assertBodyShape(firing)
        assertEquals(LogRule.SILENT_ALL, firing.rule)
        assertEquals(AlertSeverity.WARNING, firing.severity)
        assertEquals("log:*:silent", firing.key)
        assertEquals("*", firing.server)
        assertNull(firing.targetName)
        assertTrue(firing.detail.contains("45분"), firing.detail)
        val resolved = LogAlerts.silentAll(now, Duration.ofMinutes(45), now, resolved = true)
        assertEquals("log:*:silent:resolved", resolved.key)
        assertEquals(AlertSeverity.INFO, resolved.severity)
    }

    @Test
    fun `로그를 한 번도 받지 않았어도 알림을 만들고 글에 null 이 찍히지 않는다`() {
        for (alert in listOf(LogAlerts.silent("backend", "ci-agent-158", null, threshold, now, resolved = false),
                LogAlerts.silentAll(null, threshold, now, resolved = false))) {
            assertBodyShape(alert)
            assertFalse(alert.title.contains("null") || alert.detail.contains("null"), "글에 null 이 찍혔다: ${alert.title} / ${alert.detail}")
        }
    }

    @Test
    fun `서비스 · 서버 이름의 제어 문자 · 구분 문자 · 서식 문자는 key · title · detail 어디에도 남지 않고 물음표로 바뀐다`() {
        val service = "back\nend\r\t\u0000\u001b\u007f\u0085  ​‮﻿x"
        val server = "ci\nagent"
        for (resolved in listOf(false, true)) {
            val alert = LogAlerts.silent(service, server, lastLog, threshold, now, resolved)
            assertBodyShape(alert)
            for (text in alert.eventBody().values) for (hidden in hiddenCharacters) {
                assertFalse(text.contains(hidden), "알림의 글에 U+%04X 가 남았다: %s".format(hidden.code, text))
            }
            assertTrue(alert.title.contains("back?end"), "서비스 이름의 줄바꿈이 '?' 로 바뀌지 않았다: ${alert.title}")
            assertTrue(alert.key.startsWith("log:back?end"), "key 의 줄바꿈이 '?' 로 바뀌지 않았다: ${alert.key}")
            assertFalse(alert.detail.contains("ci\nagent"), "서버 이름의 줄바꿈이 detail 에 남았다")
        }
    }

    @Test
    fun `긴 서비스 이름도 key 는 120자 이하이고 같은 서비스는 같은 key · 다른 서비스는 다른 key · 해소는 넘음 key 와 겹치지 않는다`() {
        val long = "s".repeat(300)
        val other = "s".repeat(299) + "t"
        val first = LogAlerts.silent(long, "ci-agent-158", lastLog, threshold, now, resolved = false)
        val again = LogAlerts.silent(long, "ci-agent-158", lastLog, threshold, now.plusSeconds(3600), resolved = false)
        val different = LogAlerts.silent(other, "ci-agent-158", lastLog, threshold, now, resolved = false)
        val resolved = LogAlerts.silent(long, "ci-agent-158", now, threshold, now.plusSeconds(60), resolved = true)
        for (alert in listOf(first, again, different, resolved)) assertBodyShape(alert)
        assertEquals(first.key, again.key, "같은 서비스의 key 가 바뀌었다")
        assertNotEquals(first.key, different.key, "다른 서비스의 key 가 같다")
        assertEquals(first.key + ":resolved", resolved.key)
        assertTrue(first.key.startsWith("log:sss"), first.key)
    }
}
