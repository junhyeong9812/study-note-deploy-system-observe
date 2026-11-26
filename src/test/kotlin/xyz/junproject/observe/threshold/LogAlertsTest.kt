package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

/**
 * 로그 알림의 몸([LogAlerts]) — 알림 서버로 가는 꼴은 자원 알림과 같다(`{source:"observe", kind, severity, key, title, detail}`):
 * - 서비스별(`log_silent`): key `log:<서비스>:silent`(해소는 뒤에 `:resolved`), 서버 = 그 서비스가 사는 서버, 대상 이름 = 서비스(다듬은 글자).
 *   이름을 다듬어 글자가 바뀌거나 이름에 `~` 가 들면 key 는 `log:<다듬은 이름>~<원래 이름 UTF-8 의 SHA-256 앞 12자(소문자 16진)>:silent` —
 *   숨은 문자만 다른 이름 · 글자 그대로의 `?` 가 든 이름이 한 key 가 되지 않는다. key 는 서비스가 정한다(서버는 key 에 들지 않는다 — 상태의 열쇠가 서비스 · 규칙이다).
 *   빈 이름도 그대로(`log::silent`). 120자를 넘으면 지금처럼 앞부분과 전체의 해시로 줄인다(서로게이트 쌍을 쪼개지 않는다).
 * - 전체 한 줄(`log_silent_all`): key `log-all:silent`(서비스의 자리 `log:` 와 다른 글자 — 서비스 이름이 무엇이든 겹치지 않는다), 서버 `*`, 대상 이름 없음.
 * - 서로 다른 서비스 · 전체 한 줄의 넘음 · 해소는 key 도, 같은 시각의 고유 키도 모두 서로 다르다.
 * - title · detail 은 상한(알림 서버와 같이 UTF-16 단위 — 200 · 1500) 안에서 글자(코드포인트)의 경계로 자른다 — 짝 없는 서로게이트가 남지 않는다.
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
    fun `전체 한 줄의 무수신은 log_silent_all 규칙 · 서버 별표 · 대상 이름 없음 · key log-all silent 다`() {
        val firing = LogAlerts.silentAll(lastLog, Duration.ofMinutes(45), now, resolved = false)
        assertBodyShape(firing)
        assertEquals(LogRule.SILENT_ALL, firing.rule)
        assertEquals(AlertSeverity.WARNING, firing.severity)
        assertEquals("log-all:silent", firing.key)
        assertEquals("*", firing.server)
        assertNull(firing.targetName)
        assertTrue(firing.detail.contains("45분"), firing.detail)
        val resolved = LogAlerts.silentAll(now, Duration.ofMinutes(45), now, resolved = true)
        assertEquals("log-all:silent:resolved", resolved.key)
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

    // ── 서로 다른 대상은 key · 고유 키가 겹치지 않는다 ──────────

    private fun originalHash(name: String) =
        MessageDigest.getInstance("SHA-256").digest(name.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }.take(12)

    private fun assertNoLoneSurrogate(text: String, what: String) = text.forEachIndexed { index, character ->
        if (Character.isHighSurrogate(character)) assertTrue(index + 1 < text.length && Character.isLowSurrogate(text[index + 1]), "$what 에 짝 없는 상위 서로게이트(위치 $index)")
        if (Character.isLowSurrogate(character)) assertTrue(index > 0 && Character.isHighSurrogate(text[index - 1]), "$what 에 짝 없는 하위 서로게이트(위치 $index)")
    }

    @Test
    fun `다듬어도 글자가 그대로이고 물결표가 없는 이름은 key 가 log 이름 silent 그대로다`() {
        for (name in listOf("backend", "*", "x:silent", "log-all", "all", "a?b", "", "백엔드", "\uD83D\uDE00svc")) {
            assertEquals("log:$name:silent", LogAlerts.silent(name, "ci-agent-158", lastLog, threshold, now, resolved = false).key, "'$name'")
            assertEquals("log:$name:silent:resolved", LogAlerts.silent(name, "ci-agent-158", lastLog, threshold, now, resolved = true).key, "'$name'")
        }
    }

    @Test
    fun `다듬어 글자가 바뀌거나 물결표가 든 이름은 key 의 다듬은 이름 뒤에 원래 이름의 해시 12자가 붙는다`() {
        val cases = mapOf("a\u200Bb" to "a?b", "a\u200Cb" to "a?b", "a\nb" to "a?b", "a\rb" to "a?b", "a~b" to "a~b", "a?b~${originalHash("a\u200Bb")}" to "a?b~${originalHash("a\u200Bb")}")
        for ((name, shown) in cases) {
            val alert = LogAlerts.silent(name, "ci-agent-158", lastLog, threshold, now, resolved = false)
            assertEquals("log:$shown~${originalHash(name)}:silent", alert.key, "'${name.map { it.code }}'")
            assertEquals(shown, alert.targetName, "대상 이름은 다듬은 글자다")
        }
    }

    @Test
    fun `서로 다른 서비스와 전체 한 줄의 넘음 · 해소는 어떤 이름에서도 key 와 같은 시각의 고유 키가 모두 서로 다르다`() {
        val longPrefix = "p".repeat(200)
        val names = listOf("*", "x", "x:silent", "log-all", "all", "a?b", "a\u200Bb", "a\u200Cb", "a\nb", "a\rb", "a~b", "a?b~${originalHash("a\u200Bb")}",
            longPrefix + "1", longPrefix + "2", "백엔드", "\uD83D\uDE00", "", "log:x", "x:silent:resolved")
        val servers = listOf("ci-agent-158", "ci-agent-9")
        val byName = names.associateWith { name ->
            servers.flatMap { server -> listOf(false, true).map { resolved -> LogAlerts.silent(name, server, lastLog, threshold, now, resolved) } }
        }
        for ((name, alerts) in byName) {
            assertEquals(1, alerts.filterNot { it.key.endsWith(":resolved") }.map { it.key }.toSet().size, "'$name' 의 넘음 key 가 서버마다 다르다 — key 는 서비스가 정한다")
            for (alert in alerts) {
                assertTrue(alert.key.length <= 120, "key 가 120자를 넘는다: ${alert.key}")
                assertNoLoneSurrogate(alert.key, "key")
            }
        }
        val everyKind = byName.values.map { alerts -> alerts.filter { it.server == servers.first() } }.flatten() +
            listOf(LogAlerts.silentAll(lastLog, threshold, now, resolved = false), LogAlerts.silentAll(lastLog, threshold, now, resolved = true))
        val keys = everyKind.map { it.key }
        assertEquals(keys.size, keys.toSet().size, "key 가 겹친다: ${keys.groupBy { it }.filterValues { it.size > 1 }.keys}")
        val eventIds = everyKind.map { it.eventId }
        assertEquals(eventIds.size, eventIds.toSet().size, "같은 시각의 고유 키가 겹친다: ${eventIds.groupBy { it }.filterValues { it.size > 1 }.keys}")
    }

    @Test
    fun `title 과 detail 은 상한 안에서 글자의 경계로 잘려 짝 없는 서로게이트가 남지 않는다`() {
        val emojiName = "a" + "\uD83D\uDE00".repeat(800)
        for (resolved in listOf(false, true)) {
            val alert = LogAlerts.silent(emojiName, "ci-agent-158", lastLog, threshold, now, resolved)
            assertTrue(alert.title.length in 199..200, "title 이 상한(UTF-16 200) 안에서 가장 길게 잘리지 않았다(${alert.title.length})")
            assertTrue(alert.detail.length in 1499..1500, "detail 이 상한(UTF-16 1500) 안에서 가장 길게 잘리지 않았다(${alert.detail.length})")
            assertNoLoneSurrogate(alert.title, "title")
            assertNoLoneSurrogate(alert.detail, "detail")
            assertNoLoneSurrogate(alert.key, "key")
            for (text in alert.eventBody().values) assertNoLoneSurrogate(text, "몸")
        }
    }
}
