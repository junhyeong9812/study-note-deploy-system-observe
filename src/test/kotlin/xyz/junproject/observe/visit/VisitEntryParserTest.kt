package xyz.junproject.observe.visit

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.MetricStreamRecord
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.SEARCH_WORDS
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE

/**
 * 스트림 항목의 꼴(계약) — 칸: `v`(= "1") · `path` · `screen` · `referrer_host`(없어도 된다) · `device` · `visitor` · `ip`.
 * 방문 시각은 스트림 ID 의 밀리초다(front 가 싣지 않는다). 꼴이 틀리면 그 항목만 버리고 이유를 센다.
 * 검사 순서: ID → 칸 없음 → 필수 칸 → 버전 → 제어 문자 → 길이 → 칸마다의 꼴(경로 → 화면 → 들어온 곳 → 기기 → 구분값 → 주소).
 * 경로는 **먼저 자르고 나서 검사한다**: 처음 나오는 `?` · `#` · `;` 의 뒤(검색어 · 조각 · `;jsessionid=…` 같은 세션 값)는 버리고 앞까지만 저장한다 —
 * 항목은 받고 자른 수에 센다. 자른 뒤의 경로에 제어 문자(Cc)나 서식 문자(Cf — 글 방향 뒤집기 · 폭 없는 빈칸)가 있으면 `control_character`,
 * 잘려 나간 부분에만 있던 것으로는 버리지 않는다. `//` 로 시작하는 경로는 `invalid_path`. 다른 칸의 제어 문자는 받은 글 그대로 본다.
 */
class VisitEntryParserTest {
    private fun parse(fields: Map<String, String>, id: String = "1764000000000-0") = VisitEntryParser.parse(MetricStreamRecord(id, fields))

    private fun accepted(fields: Map<String, String>): VisitEntryParse.Accepted {
        val result = parse(fields)
        assertTrue(result is VisitEntryParse.Accepted, "받아야 하는데 버렸다: $result")
        return result as VisitEntryParse.Accepted
    }

    private fun assertDropped(reason: VisitDropReason, fields: Map<String, String>, label: String = "$fields") =
        assertEquals(VisitEntryParse.Dropped(reason), parse(fields), label)

    // ── 받는 꼴 ────────────────────────────────────────

    @Test
    fun `맞는 꼴의 항목은 칸 그대로 받고 방문 시각은 스트림 ID 의 밀리초다`() {
        val result = accepted(VisitRecords.fields())
        assertEquals(
            Visit(StreamId(1_764_000_000_000, 0), "/wiki/kubernetes/pod", "topic", "www.google.com", VisitDevice.DESKTOP, VISITOR_ONE, ADDRESS_V4),
            result.visit,
        )
        assertFalse(result.queryStripped)
        assertEquals(java.time.Instant.ofEpochMilli(1_764_000_000_000), result.visit.id.at)
    }

    @Test
    fun `IPv6 주소도 원문 그대로 받는다`() {
        for (address in listOf(ADDRESS_V6, "::1", "2001:0db8:0000:0000:0000:0000:0000:0077", "::ffff:203.0.113.77", "2001:DB8::77", "fe80::")) {
            assertEquals(address, accepted(VisitRecords.fields(ip = address)).visit.ip, address)
        }
    }

    @Test
    fun `기기 갈래 셋을 모두 받는다`() {
        assertEquals(VisitDevice.DESKTOP, accepted(VisitRecords.fields(device = "desktop")).visit.device)
        assertEquals(VisitDevice.MOBILE, accepted(VisitRecords.fields(device = "mobile")).visit.device)
        assertEquals(VisitDevice.BOT, accepted(VisitRecords.fields(device = "bot")).visit.device)
    }

    @Test
    fun `들어온 곳이 없거나 빈 글자면 직접(null)이다`() {
        assertNull(accepted(VisitRecords.fields(referrerHost = null)).visit.referrerHost)
        assertNull(accepted(VisitRecords.fields(referrerHost = "")).visit.referrerHost)
    }

    @Test
    fun `모르는 칸(User-Agent 원문 등)은 무시하고 저장할 방문에 싣지 않는다`() {
        val withExtra = VisitRecords.fields() + mapOf("user_agent" to "Mozilla/5.0 (X11; Linux x86_64)", "cookie" to "study_visitor=abc")
        assertEquals(accepted(VisitRecords.fields()).visit, accepted(withExtra).visit)
    }

    @Test
    fun `한글 경로를 받는다`() {
        assertEquals("/wiki/쿠버네티스/파드", accepted(VisitRecords.fields(path = "/wiki/쿠버네티스/파드")).visit.path)
    }

    // ── 검색어(쿼리 문자열)는 남기지 않는다 ─────────────

    @Test
    fun `경로에 쿼리가 섞여 오면 물음표 앞까지만 저장하고 잘라 냈다고 표시한다`() {
        val result = accepted(VisitRecords.fields(path = "/search?q=$SEARCH_WORDS&page=2", screen = "search"))
        assertEquals("/search", result.visit.path)
        assertTrue(result.queryStripped)
        assertFalse(result.visit.toString().contains(SEARCH_WORDS))
    }

    @Test
    fun `조각(샵 뒤)도 잘라 낸다`() {
        val result = accepted(VisitRecords.fields(path = "/wiki/a#$SEARCH_WORDS"))
        assertEquals("/wiki/a", result.visit.path)
        assertTrue(result.queryStripped)
    }

    @Test
    fun `물음표가 맨 앞이라 잘라 낸 경로가 비면 경로의 꼴이 틀린 것이다`() {
        assertDropped(VisitDropReason.INVALID_PATH, VisitRecords.fields(path = "?q=$SEARCH_WORDS"))
    }

    @Test
    fun `경로 길이 상한은 잘라 낸 뒤의 경로로 잰다 — 512 는 받고 513 은 too_long`() {
        val longest = "/" + "a".repeat(VisitEntryParser.MAX_PATH_LENGTH - 1)
        assertEquals(longest, accepted(VisitRecords.fields(path = longest)).visit.path)
        assertEquals(longest, accepted(VisitRecords.fields(path = "$longest?q=$SEARCH_WORDS")).visit.path)
        assertDropped(VisitDropReason.TOO_LONG, VisitRecords.fields(path = longest + "a"))
    }

    // ── 경로의 꼴 — 먼저 자르고 나서 검사한다 ─────────────

    /** 해석 결과를 표의 칸 하나로 — 받으면 저장할 경로와 잘랐는지, 버리면 이유 */
    private fun pathOutcome(path: String): String = when (val result = parse(VisitRecords.fields(path = path))) {
        is VisitEntryParse.Accepted -> "받음 ${visible(result.visit.path)} 자름=${result.queryStripped}"
        is VisitEntryParse.Dropped -> "버림 ${result.reason.wireName}"
    }

    /** 실패 글에서 보이지 않는 글자를 알아볼 수 있게 */
    private fun visible(text: String) = text.map { if (Character.isISOControl(it) || Character.getType(it) == Character.FORMAT.toInt()) "\\u%04x".format(it.code) else "$it" }.joinToString("")

    @Test
    fun `경로의 꼴 표 — 세미콜론 물음표 샵 뒤를 자르고 자른 뒤에 제어 문자와 서식 문자를 보고 빗금 둘로 시작하면 버린다`() {
        val table = listOf(
            "/a;jsessionid=x" to "받음 /a 자름=true",
            "/a;b?c#d" to "받음 /a 자름=true",
            "/a?q=\u0001" to "받음 /a 자름=true",                      // 잘려 나간 부분에만 제어 문자
            "/a#\u0000" to "받음 /a 자름=true",
            "/a;\u202Eb" to "받음 /a 자름=true",                       // 잘려 나간 부분에만 서식 문자
            "/a\u0001b" to "버림 control_character",
            "/a\u202Eb" to "버림 control_character",                    // 글 방향 뒤집기(RIGHT-TO-LEFT OVERRIDE)
            "/a\u200Bb" to "버림 control_character",                    // 폭 없는 빈칸(ZERO WIDTH SPACE)
            "/a\u2066b" to "버림 control_character",                    // 글 방향 격리(LEFT-TO-RIGHT ISOLATE)
            "/a\uFEFFb" to "버림 control_character",                    // 폭 없는 붙임 빈칸(BOM)
            "/a\u0001b?q=x" to "버림 control_character",                // 자른 뒤에도 남는 제어 문자
            "//evil.example/x" to "버림 invalid_path",
            "//" to "버림 invalid_path",
            "//?x" to "버림 invalid_path",
            ";x" to "버림 invalid_path",
            "/" to "받음 / 자름=false",
            "/a/b" to "받음 /a/b 자름=false",
            "/wiki/쿠버네티스/파드" to "받음 /wiki/쿠버네티스/파드 자름=false",
            "/;x" to "받음 / 자름=true",
        )
        val wrong = table.mapNotNull { (path, expected) ->
            val actual = pathOutcome(path)
            if (actual == expected) null else "${visible(path)}: 기대 [$expected] 실제 [$actual]"
        }
        assertTrue(wrong.isEmpty(), "어긋난 줄 ${wrong.size}개:\n" + wrong.joinToString("\n"))
    }

    @Test
    fun `세미콜론 뒤를 자른 경로의 세션 값은 저장할 방문 어디에도 없다`() {
        val result = accepted(VisitRecords.fields(path = "/wiki/a;jsessionid=$SEARCH_WORDS"))
        assertEquals("/wiki/a", result.visit.path)
        assertTrue(result.queryStripped)
        assertFalse(result.visit.toString().contains(SEARCH_WORDS))
    }

    @Test
    fun `경로 길이 상한은 세미콜론 뒤를 자른 뒤의 경로로 잰다`() {
        val longest = "/" + "a".repeat(VisitEntryParser.MAX_PATH_LENGTH - 1)
        assertEquals(longest, accepted(VisitRecords.fields(path = "$longest;jsessionid=$SEARCH_WORDS")).visit.path)
    }

    // ── 버리는 꼴 ──────────────────────────────────────

    @Test
    fun `스트림 ID 꼴이 아니면 invalid_id 이고 칸이 하나도 없으면 trimmed`() {
        assertEquals(VisitEntryParse.Dropped(VisitDropReason.INVALID_ID), parse(VisitRecords.fields(), id = "not-an-id"))
        assertEquals(VisitEntryParse.Dropped(VisitDropReason.TRIMMED), parse(emptyMap()))
    }

    @Test
    fun `필수 칸이 빠지거나 빈 글자면 missing_field`() {
        for (field in listOf("v", "path", "screen", "device", "visitor", "ip")) {
            assertDropped(VisitDropReason.MISSING_FIELD, VisitRecords.fields() - field, "$field 빠짐")
            assertDropped(VisitDropReason.MISSING_FIELD, VisitRecords.fields() + (field to ""), "$field 빈 글자")
        }
    }

    @Test
    fun `버전이 1 이 아니면 unsupported_version`() {
        assertDropped(VisitDropReason.UNSUPPORTED_VERSION, VisitRecords.fields(version = "2"))
    }

    @Test
    fun `어느 칸이든 NUL 같은 제어 문자가 있으면 control_character`() {
        for (field in listOf("path", "screen", "referrer_host", "device", "visitor", "ip")) {
            val original = VisitRecords.fields().getValue(field)
            assertDropped(VisitDropReason.CONTROL_CHARACTER, VisitRecords.fields() + (field to original + "\u0000"), "$field NUL")
        }
        assertDropped(VisitDropReason.CONTROL_CHARACTER, VisitRecords.fields(path = "/wiki/a\tb"))
        assertDropped(VisitDropReason.CONTROL_CHARACTER, VisitRecords.fields(path = "/wiki/a\u007f"))
    }

    @Test
    fun `경로가 빗금으로 시작하지 않거나 빈칸이 있으면 invalid_path`() {
        assertDropped(VisitDropReason.INVALID_PATH, VisitRecords.fields(path = "wiki/a"))
        assertDropped(VisitDropReason.INVALID_PATH, VisitRecords.fields(path = "https://example.com/wiki/a"))
        assertDropped(VisitDropReason.INVALID_PATH, VisitRecords.fields(path = "/wiki/a b"))
    }

    @Test
    fun `화면 종류는 소문자로 시작하는 32자까지 — 33자는 too_long 이고 대문자는 invalid_screen`() {
        val longest = "a".repeat(VisitEntryParser.MAX_SCREEN_LENGTH)
        assertEquals(longest, accepted(VisitRecords.fields(screen = longest)).visit.screen)
        assertEquals("search-result_2", accepted(VisitRecords.fields(screen = "search-result_2")).visit.screen)
        assertDropped(VisitDropReason.TOO_LONG, VisitRecords.fields(screen = longest + "a"))
        assertDropped(VisitDropReason.INVALID_SCREEN, VisitRecords.fields(screen = "Topic"))
        assertDropped(VisitDropReason.INVALID_SCREEN, VisitRecords.fields(screen = "2topic"))
    }

    @Test
    fun `들어온 곳은 소문자 호스트 이름만 — 253자는 받고 254자는 too_long`() {
        val longest = listOf("a".repeat(63), "b".repeat(63), "c".repeat(63), "d".repeat(61)).joinToString(".")
        assertEquals(253, longest.length)
        assertEquals(longest, accepted(VisitRecords.fields(referrerHost = longest)).visit.referrerHost)
        assertDropped(VisitDropReason.TOO_LONG, VisitRecords.fields(referrerHost = longest + "d"))
        for (bad in listOf("Google.com", "google.com:443", "https://google.com", "google.com/search", "-google.com", "google..com", "[2001:db8::1]")) {
            assertDropped(VisitDropReason.INVALID_REFERRER, VisitRecords.fields(referrerHost = bad), bad)
        }
    }

    @Test
    fun `기기 갈래가 셋 밖이면 invalid_device`() {
        for (bad in listOf("tablet", "Desktop", "BOT")) assertDropped(VisitDropReason.INVALID_DEVICE, VisitRecords.fields(device = bad), bad)
    }

    @Test
    fun `구분값은 소문자 16진 64자 — 63자 · 65자 · 대문자 · 16진 밖은 invalid_visitor`() {
        for (bad in listOf(VISITOR_ONE.dropLast(1), VISITOR_ONE + "a", VISITOR_ONE.uppercase(), "g" + VISITOR_ONE.drop(1))) {
            assertDropped(VisitDropReason.INVALID_VISITOR, VisitRecords.fields(visitor = bad), bad)
        }
    }

    @Test
    fun `주소 꼴이 아니면 invalid_ip 이고 45자를 넘으면 too_long`() {
        for (bad in listOf("203.0.113", "203.0.113.256", "203.0.113.77:443", "010.0.113.77", " 203.0.113.77", "localhost", "example.com",
                "2001:db8::77::1", "2001:db8:::77", "12345::1", "2001:db8::g", "fe80::1%eth0", "[2001:db8::77]", "203.0.113.77, 198.51.100.1")) {
            assertDropped(VisitDropReason.INVALID_IP, VisitRecords.fields(ip = bad), bad)
        }
        assertDropped(VisitDropReason.TOO_LONG, VisitRecords.fields(ip = "1".repeat(VisitEntryParser.MAX_IP_LENGTH + 1)))
    }

    // ── 주소 원문이 새지 않게 ─────────────────────────────

    @Test
    fun `방문의 글자 표현(toString)에 주소와 구분값이 없다 — 실수로 로그에 찍혀도 새지 않게`() {
        for (address in listOf(ADDRESS_V4, ADDRESS_V6)) {
            val text = accepted(VisitRecords.fields(ip = address)).toString()
            assertFalse(text.contains(address), text)
            assertFalse(text.contains(VISITOR_ONE), text)
        }
    }
}
