package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.ResourceAlert
import java.time.Duration
import java.time.Instant

/**
 * ⑪-6a 글자 규칙의 판정(주기 판정 — 무수신 판정과 같은 30초 바퀴): 규칙마다 따로, 이번에 볼 줄은 시각이
 * `(max(앞에서 본 곳, now − 5분), now − 15초]`(받은 때보다 5분 넘게 오래된 줄은 보지 않는다 · 15초는 저장이 따라올 여유) ·
 * 첫 맞춤은 그 바퀴에 한 건 · 그 뒤 10분 안의 맞춤은 쌓기만 하다가 마지막 알림에서 10분이 지난 첫 바퀴에 쌓인 수로 한 건(맞춤이 그친 뒤에도) ·
 * 앞 상태 중 자기 설정에 없는 규칙은 그대로. 줄은 가짜(시각 · 서비스 · 내용 · 요청 ID) — 글자 그대로의 맞춤은 저장소의 엔진 테스트가 본다.
 */
class LogPatternJudgeTest {
    private val start = Instant.parse("2026-11-29T03:00:00Z")
    private val limit = LogPattern("claude_limit", "backend", AlertSeverity.CRITICAL, "result=limit_reached")
    private val auto = LogPattern("claude_auto", "backend", AlertSeverity.WARNING, "result=answered auto=true")
    private val judge = LogPatternJudge(LogPatternSettings(listOf(limit, auto), emptyList()))

    private data class Line(val at: Instant, val service: String, val message: String, val requestId: String)

    /** 가짜 줄 — 서비스가 같고 내용에 글자가 그대로 든 줄을 `(after, until]` 에서 센다. 부른 기간을 남긴다 */
    private inner class Lines : LogPatternLines {
        val lines = mutableListOf<Line>()
        val asked = mutableListOf<Triple<String, Instant, Instant>>()
        override fun matches(service: String, text: String, after: Instant, until: Instant): LogPatternMatches {
            asked += Triple(text, after, until)
            val found = lines.filter { it.service == service && it.message.contains(text) && it.at.isAfter(after) && !it.at.isAfter(until) }.sortedBy { it.at }
            return LogPatternMatches(found.size.toLong(), found.firstOrNull()?.requestId, found.firstOrNull()?.at)
        }
    }

    private val lines = Lines()
    private var states: List<LogPatternState> = emptyList()
    private fun round(now: Instant): List<ResourceAlert> = judge.judge(now, states, lines).also { states = it.states }.events

    private fun add(at: Instant, message: String = "claude run: result=limit_reached auto=false duration_ms=12", requestId: String = "req-a", service: String = "backend") {
        lines.lines += Line(at, service, message, requestId)
    }

    @Test
    fun `첫 맞춤은 그 바퀴에 한 건이고 줄 수 · 첫 줄의 요청 ID · 시각을 싣는다`() {
        add(start.plusSeconds(1), requestId = "req-first")
        add(start.plusSeconds(2), requestId = "req-second")
        val events = round(start.plusSeconds(30))
        assertEquals(1, events.size)
        val event = events.single()
        assertEquals("log_pattern:claude_limit", event.key)
        assertEquals(AlertSeverity.CRITICAL, event.severity)
        assertEquals("2줄 맞음 · 첫 줄 요청 ID req-first · 시각 ${start.plusSeconds(1)}", event.detail)
    }

    @Test
    fun `10분 안의 맞춤은 쌓기만 하고 마지막 알림에서 10분이 지난 첫 바퀴에 쌓인 수로 한 건 — 정확히 10분이면 낸다`() {
        add(start.plusSeconds(1))
        assertEquals(1, round(start.plusSeconds(30)).size)                         // 첫 알림 — start+30s
        add(start.plusSeconds(60), requestId = "req-later-1")
        add(start.plusSeconds(120), requestId = "req-later-2")
        assertEquals(0, round(start.plusSeconds(90)).size)
        assertEquals(0, round(start.plusSeconds(150)).size)
        assertEquals(0, round(start.plusSeconds(30).plus(Duration.ofMinutes(10)).minusMillis(1)).size, "10분이 되기 전에 다시 냈다")
        val events = round(start.plusSeconds(30).plus(Duration.ofMinutes(10)))   // 맞춤이 그친 뒤에도 창이 지나면 낸다
        assertEquals(listOf("2줄 맞음 · 첫 줄 요청 ID req-later-1 · 시각 ${start.plusSeconds(60)}"), events.map { it.detail })
        assertEquals(0, round(start.plusSeconds(30).plus(Duration.ofMinutes(11))).size, "쌓인 것이 없는데 냈다")
    }

    @Test
    fun `규칙마다 따로 센다 — 한 규칙의 창이 다른 규칙의 첫 알림을 막지 않는다`() {
        add(start.plusSeconds(1))
        assertEquals(listOf("log_pattern:claude_limit"), round(start.plusSeconds(30)).map { it.key })
        add(start.plusSeconds(40), message = "claude run: result=answered auto=true duration_ms=3")
        assertEquals(listOf("log_pattern:claude_auto"), round(start.plusSeconds(60)).map { it.key })
    }

    @Test
    fun `한 줄이 두 규칙에 맞으면 둘 다 낸다 · 다른 서비스 · 다른 글자는 맞지 않는다`() {
        val both = LogPatternJudge(LogPatternSettings(listOf(limit, LogPattern("any_claude", "backend", AlertSeverity.INFO, "claude run:")), emptyList()))
        add(start.plusSeconds(1))
        add(start.plusSeconds(2), service = "front")
        add(start.plusSeconds(3), message = "claude run: result=answered auto=false")
        val events = both.judge(start.plusSeconds(30), emptyList(), lines).events
        assertEquals(mapOf("log_pattern:claude_limit" to "1줄", "log_pattern:any_claude" to "2줄"), events.associate { it.key to it.detail.substringBefore(" 맞음") })
    }

    @Test
    fun `받은 때보다 5분 넘게 오래된 줄은 보지 않는다 — 처음 보는 규칙도 · 오래 쉬었다 돌아온 판정도`() {
        add(start.minus(Duration.ofMinutes(5)).minusSeconds(1).plusSeconds(30))      // 판정 시각 기준 5분 1초 전
        assertEquals(0, round(start.plusSeconds(30)).size, "5분 넘게 오래된 줄을 판정했다")
        val asked = lines.asked.first()
        assertEquals(start.plusSeconds(30).minus(Duration.ofMinutes(5)), asked.second, "처음 보는 규칙의 시작이 now − 5분이 아니다")
        assertEquals(start.plusSeconds(15), asked.third, "끝이 now − 15초가 아니다")
        // 판정이 한참 멈췄다 돌아오면 앞에서 본 곳이 아니라 now − 5분부터
        lines.asked.clear()
        add(start.plus(Duration.ofHours(1)), message = "result=limit_reached")
        val later = start.plus(Duration.ofHours(1)).plus(Duration.ofMinutes(6))
        assertEquals(0, round(later).size, "판정이 멈춘 동안의 밀린 줄을 판정했다")
        assertEquals(later.minus(Duration.ofMinutes(5)), lines.asked.first().second)
    }

    @Test
    fun `바퀴 사이의 줄은 빠짐도 겹침도 없다 — 앞 바퀴의 끝부터 이번 바퀴의 끝까지`() {
        round(start.plusSeconds(30))
        round(start.plusSeconds(60))
        val periods = lines.asked.filter { it.first == limit.text }
        assertEquals(periods[0].third, periods[1].second)
        assertEquals(start.plusSeconds(45), periods[1].third)
        add(start.plusSeconds(15).plusMillis(1))                                    // 첫 바퀴의 끝(+15s) 바로 뒤 — 둘째 바퀴가 센다
        add(start.plusSeconds(15))                                                  // 첫 바퀴의 끝 그 자리 — 첫 바퀴의 몫이라 다시 세지 않는다
        states = emptyList()
        lines.asked.clear()
        val fresh = LogPatternJudge(LogPatternSettings(listOf(limit), emptyList()))
        val first = fresh.judge(start.plusSeconds(30), emptyList(), lines)
        assertEquals("1줄", first.events.single().detail.substringBefore(" 맞음"))
        val second = fresh.judge(start.plusSeconds(60), first.states, lines)                 // 경계 바로 뒤의 줄을 센다(창 안 — 쌓기만)
        assertEquals(emptyList<ResourceAlert>(), second.events)
        val third = fresh.judge(start.plusSeconds(30).plus(Duration.ofMinutes(10)), second.states, lines)
        assertEquals("1줄", third.events.single().detail.substringBefore(" 맞음"), "경계의 줄을 빠뜨렸거나 두 번 셌다")
    }

    @Test
    fun `시각이 앞선 줄은 그 시각이 판정의 끝을 지난 바퀴에 센다`() {
        add(start.plus(Duration.ofMinutes(2)))                                     // 받은 때보다 2분 앞선 줄
        assertEquals(0, round(start.plusSeconds(30)).size)
        assertEquals(1, round(start.plus(Duration.ofMinutes(2)).plusSeconds(30)).size)
    }

    @Test
    fun `자기 설정에 없는 규칙의 앞 상태는 그대로 돌려준다`() {
        val other = LogPatternState("other_place_rule", start, start, 3, "req-x", start)
        val result = judge.judge(start.plusSeconds(30), listOf(other), lines)
        assertTrue(other in result.states)
    }
}
