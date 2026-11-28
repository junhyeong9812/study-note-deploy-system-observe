package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.threshold.LogAlerts
import xyz.junproject.observe.threshold.LogRule
import java.time.Duration
import java.time.Instant

/**
 * 무수신 판정의 규칙(서비스 · 규칙마다 상태 하나 — 전체 한 줄은 서비스 `*` · 규칙 `log_silent_all`):
 *
 * | 앞 상태 | 지금 | 서버 끊김 | 알림 | 뒤 상태 |
 * |---|---|---|---|---|
 * | 넘지 않음 | 조용함(마지막 로그가 기준보다 **엄격히** 오래 · 받은 기록 없음) | 아님 | 넘음 | 넘음 · since = 알림 시각 = 지금 |
 * | 넘지 않음 | 조용함 | 끊김 | 없음 | 넘음 · since = 지금 · 알림 시각 없음(아직 알리지 않음) |
 * | 넘음(알리지 않음) | 조용함 | 아님 | 넘음(처음 알림) | 알림 시각 = 지금 · since 그대로 |
 * | 넘음(알림) | 조용함 | 아님 | 마지막 알림에서 1시간이 지났으면(같으면 지난 것) `(계속)` 다시 알림 | 알림 시각 = 지금 |
 * | 넘음(알림) | 조용함 | 끊김 | 없음(다시 알림도 없음) | 그대로 |
 * | 넘음(알림) | 받음 | — | 해소 | 넘지 않음 · 알림 시각 = 지금 |
 * | 넘음(알리지 않음) | 받음 | — | 없음(알린 적 없는 넘음은 해소를 알리지 않는다) | 넘지 않음 |
 * | 아무것 | 보류(판정하지 않음) | — | 없음(새로 울리지도 · 해소하지도 · 다시 알리지도 않는다) | 앞 상태 그대로 |
 *
 * 전체 한 줄은 서버 끊김과 무관하다. 자기 설정에 없는 서비스 · 꺼진 전체 한 줄의 상태는 손대지 않고 그대로 돌려준다(다른 자리의 설정일 수 있다 — §16-2) —
 * 다시 설정에 넣으면 그 앞 상태에서 이어 간다. 다시 알림의 글: title 앞에 `(계속) `, detail 뒤에 `\n처음 넘은 시각: <since>`.
 * 넘음 · 해소의 몸은 [LogAlerts] 그대로(고유 키 = key@판정 시각).
 */
class LogSilentJudgeTest {
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val backendThreshold = Duration.ofMinutes(30)
    private val settings = LogAlertSettings(listOf(
        WatchedService("backend", "ci-agent-158", backendThreshold),
        WatchedService("front", "ci-agent-9", Duration.ofMinutes(45)),
    ), Duration.ofMinutes(60), emptyList())
    private val judge = LogSilentJudge(settings)

    private fun input(at: Instant = now, backend: Instant? = at, front: Instant? = at, any: Instant? = at, judging: Boolean = true, silentServers: Set<String> = emptySet()) =
        LogJudgeInput(at, mapOf("backend" to backend, "front" to front), any, judging, silentServers)

    private fun state(result: LogJudgeResult, service: String) = result.states.single { it.service == service }

    private fun firing(service: String = "backend", since: Instant = now.minusSeconds(600), notified: Instant? = since, last: Instant? = null) =
        LogRuleState(service, LogRule.SILENT, true, since, notified, last)

    private fun quiet(service: String = "backend", last: Instant? = now) = LogRuleState(service, LogRule.SILENT, false, null, null, last)

    @Test
    fun `기준을 넘게 조용하면 그 서비스의 넘음 알림 하나와 넘음 상태가 생긴다`() {
        val last = now.minus(backendThreshold).minusSeconds(1)
        val result = judge.judge(input(backend = last), emptyList())
        assertEquals(listOf(LogAlerts.silent("backend", "ci-agent-158", last, backendThreshold, now, resolved = false)), result.events)
        assertEquals(LogRuleState("backend", LogRule.SILENT, true, now, now, last), state(result, "backend"))
        assertEquals(LogRuleState("front", LogRule.SILENT, false, null, null, now), state(result, "front"))
    }

    @Test
    fun `마지막 로그가 정확히 기준만큼 오래면 아직 넘지 않고 1초 더 오래면 넘는다`() {
        assertEquals(emptyList<Any>(), judge.judge(input(backend = now.minus(backendThreshold)), emptyList()).events)
        assertFalse(state(judge.judge(input(backend = now.minus(backendThreshold)), emptyList()), "backend").firing)
        assertEquals(1, judge.judge(input(backend = now.minus(backendThreshold).minusSeconds(1)), emptyList()).events.size)
    }

    @Test
    fun `로그를 한 번도 받지 않은 서비스는 조용한 것으로 보아 넘음을 알린다`() {
        val result = judge.judge(input(backend = null), emptyList())
        assertEquals(listOf(LogAlerts.silent("backend", "ci-agent-158", null, backendThreshold, now, resolved = false)), result.events)
        assertTrue(state(result, "backend").firing)
    }

    @Test
    fun `넘음이 이어지면 마지막 알림에서 1시간이 되는 바퀴에 한 번만 계속 알림을 다시 보낸다`() {
        val since = now.minus(Duration.ofHours(2))
        val notified = now.minus(Duration.ofHours(1)).plusSeconds(1)
        val last = since.minus(backendThreshold)
        assertEquals(emptyList<Any>(), judge.judge(input(backend = last), listOf(firing(since = since, notified = notified))).events, "1시간이 되기 전에 다시 알렸다")

        val atHour = notified.plus(Duration.ofHours(1))
        val reminded = judge.judge(input(at = atHour, backend = last, front = atHour, any = atHour), listOf(firing(since = since, notified = notified)))
        val base = LogAlerts.silent("backend", "ci-agent-158", last, backendThreshold, atHour, resolved = false)
        assertEquals(listOf(base.copy(title = "(계속) ${base.title}", detail = "${base.detail}\n처음 넘은 시각: $since")), reminded.events)
        assertEquals(LogRuleState("backend", LogRule.SILENT, true, since, atHour, last), state(reminded, "backend"))

        val after = judge.judge(input(at = atHour.plusSeconds(30), backend = last, front = atHour, any = atHour), reminded.states)
        assertEquals(emptyList<Any>(), after.events, "다시 알린 다음 바퀴에 또 알렸다")
    }

    @Test
    fun `넘어 있던 서비스의 로그를 다시 받으면 해소 알림 하나를 보내고 해소의 글만 읽어도 무엇이 풀렸는지 안다`() {
        val back = now.minusSeconds(20)
        val result = judge.judge(input(backend = back), listOf(firing()))
        val resolved = LogAlerts.silent("backend", "ci-agent-158", back, backendThreshold, now, resolved = true)
        assertEquals(listOf(resolved), result.events)
        assertEquals(LogRuleState("backend", LogRule.SILENT, false, null, now, back), state(result, "backend"))
        assertTrue(resolved.title.contains("backend"), resolved.title)
        assertTrue(resolved.detail.contains("ci-agent-158") && resolved.detail.contains(back.toString()), resolved.detail)
        assertEquals(emptyList<Any>(), judge.judge(input(at = now.plusSeconds(30)), result.states).events, "해소를 두 번 알렸다")
    }

    @Test
    fun `보류 중에는 새로 울리지도 해소하지도 다시 알리지도 않고 앞 상태를 그대로 둔다`() {
        val previous = listOf(firing(notified = now.minus(Duration.ofHours(2))), quiet("front", now.minus(Duration.ofHours(5))))
        val held = judge.judge(input(backend = now, front = null, any = null, judging = false), previous)
        assertEquals(emptyList<Any>(), held.events)
        assertEquals(previous.toSet(), held.states.filter { it.rule == LogRule.SILENT }.toSet())
        assertEquals(emptyList<Any>(), judge.judge(input(backend = null, judging = false), emptyList()).events, "보류 중인데 처음 보는 서비스를 울렸다")
    }

    @Test
    fun `서비스가 사는 서버가 끊김이면 넘음을 알리지 않고 상태만 남기며 서버가 돌아온 뒤에도 조용하면 그때 처음 알린다`() {
        val suppressed = judge.judge(input(backend = null, silentServers = setOf("ci-agent-158")), emptyList())
        assertEquals(emptyList<Any>(), suppressed.events, "서버가 끊겼는데 로그 무수신을 알렸다")
        assertEquals(LogRuleState("backend", LogRule.SILENT, true, now, null, null), state(suppressed, "backend"))

        val stillSuppressed = judge.judge(input(at = now.plus(Duration.ofHours(2)), backend = null, silentServers = setOf("ci-agent-158")), suppressed.states)
        assertEquals(emptyList<Any>(), stillSuppressed.events)

        val later = now.plus(Duration.ofHours(3))
        val back = judge.judge(input(at = later, backend = null, front = later, any = later), stillSuppressed.states)
        assertEquals(listOf(LogAlerts.silent("backend", "ci-agent-158", null, backendThreshold, later, resolved = false)), back.events)
        assertEquals(LogRuleState("backend", LogRule.SILENT, true, now, later, null), state(back, "backend"))
    }

    @Test
    fun `알리지 않은 넘음이 서버 끊김 동안 풀리면 해소를 알리지 않는다`() {
        val suppressed = judge.judge(input(backend = null, silentServers = setOf("ci-agent-158")), emptyList())
        val returned = judge.judge(input(at = now.plusSeconds(60), backend = now.plusSeconds(50)), suppressed.states)
        assertEquals(emptyList<Any>(), returned.events, "알린 적 없는 넘음의 해소를 알렸다")
        assertFalse(state(returned, "backend").firing)
    }

    @Test
    fun `이미 알린 넘음은 서버가 끊긴 동안 다시 알리지 않고 로그가 돌아오면 해소를 알린다`() {
        val previous = listOf(firing(notified = now.minus(Duration.ofHours(3))))
        val held = judge.judge(input(backend = null, silentServers = setOf("ci-agent-158")), previous)
        assertEquals(emptyList<Any>(), held.events, "서버가 끊긴 동안 다시 알렸다")
        val back = judge.judge(input(at = now.plusSeconds(60), backend = now.plusSeconds(55)), held.states)
        assertEquals(listOf(LogAlerts.silent("backend", "ci-agent-158", now.plusSeconds(55), backendThreshold, now.plusSeconds(60), resolved = true)), back.events)
    }

    @Test
    fun `전체 한 줄은 어느 서비스든 마지막 로그가 그 기준을 넘게 오래면 넘고 서버 끊김과 무관하다`() {
        val any = now.minus(Duration.ofMinutes(61))
        val result = judge.judge(input(backend = now, front = now, any = any, silentServers = setOf("ci-agent-158", "ci-agent-9")), emptyList())
        assertEquals(listOf(LogAlerts.silentAll(any, Duration.ofMinutes(60), now, resolved = false)), result.events)
        assertEquals(LogRuleState(LogSilentJudge.ALL_SERVICES, LogRule.SILENT_ALL, true, now, now, any),
            result.states.single { it.rule == LogRule.SILENT_ALL })
        assertEquals(emptyList<Any>(), judge.judge(input(any = now.minus(Duration.ofMinutes(60))), emptyList()).events)
    }

    @Test
    fun `자기 설정에 없는 서비스와 꺼진 전체 한 줄의 상태는 손대지 않고 그대로 돌려주며 알리지 않는다`() {
        val onlyBackend = LogSilentJudge(LogAlertSettings(listOf(WatchedService("backend", "ci-agent-158", backendThreshold)), null, emptyList()))
        val gone = firing("gone", notified = now.minus(Duration.ofHours(5)))
        val all = LogRuleState(LogSilentJudge.ALL_SERVICES, LogRule.SILENT_ALL, true, now.minus(Duration.ofHours(2)), now.minus(Duration.ofHours(2)), null)
        val previous = listOf(quiet("backend"), gone, all)
        val result = onlyBackend.judge(LogJudgeInput(now, mapOf("backend" to now), now, true, emptySet()), previous)
        assertEquals(emptyList<Any>(), result.events, "설정에 없는 서비스 · 꺼진 줄을 알렸다(다시 알림 · 해소)")
        assertTrue(result.states.contains(gone), "설정에 없는 서비스의 상태를 지웠거나 바꿨다: ${result.states}")
        assertTrue(result.states.contains(all), "꺼진 전체 한 줄의 상태를 지웠거나 바꿨다: ${result.states}")
        assertEquals(3, result.states.size, "서비스 · 규칙마다 상태가 하나가 아니다: ${result.states}")
        assertEquals(1, result.states.count { it.service == "backend" })
    }

    @Test
    fun `다시 설정에 넣은 서비스는 앞 상태에서 이어 가 받고 있으면 해소 한 번 · 여전히 조용하면 다시 알림의 규칙대로다`() {
        val announced = firing("backend", since = now.minus(Duration.ofHours(3)), notified = now.minus(Duration.ofHours(2)))
        val back = now.minusSeconds(10)
        val resolved = judge.judge(input(backend = back), listOf(announced))
        assertEquals(listOf(LogAlerts.silent("backend", "ci-agent-158", back, backendThreshold, now, resolved = true)), resolved.events)

        val stillQuiet = judge.judge(input(backend = null), listOf(announced))
        val base = LogAlerts.silent("backend", "ci-agent-158", null, backendThreshold, now, resolved = false)
        assertEquals(listOf(base.copy(title = "(계속) ${base.title}", detail = "${base.detail}\n처음 넘은 시각: ${announced.since}")), stillQuiet.events,
            "앞 상태(알린 넘음 · 2시간 전)에서 이어 가지 않았다")
        val recent = firing("backend", since = now.minus(Duration.ofHours(3)), notified = now.minusSeconds(600))
        assertEquals(emptyList<Any>(), judge.judge(input(backend = null), listOf(recent)).events, "앞 상태의 알림 시각을 무시하고 다시 알렸다")
    }
}
