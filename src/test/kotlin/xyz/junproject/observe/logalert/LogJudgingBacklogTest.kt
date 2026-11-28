package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.StreamRecord
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.RoundOutcome
import java.time.Duration
import java.time.Instant

/**
 * 수집이 다시 밀린 동안에는 판정하지 않는다(§16-1) — 실제 수집기([LogCollector]) · 판정의 문([LogJudgingGate]) · 판정 한 바퀴([LogAlertRound])를 이어,
 * 로그는 가짜 스트림 · 저장소(마지막 로그 = 저장된 줄의 스트림 시각)로:
 * 계속 로그를 보내던 서비스가 수집이 끊긴 동안 밀린 줄 뒤에 있으면, 돌아온 첫 성공 바퀴(상한만큼 읽음)에는 판정하지 않고, 다 따라잡은 뒤 판정해 알림이 0 이다.
 * 로그가 끊임없이 와도 한 바퀴의 상한보다 적게 읽는 수집이면 판정한다(빈 읽기가 없어도) · 뜬 뒤 15분이 지나도 상한만큼씩만 읽으면 판정하지 않고 헬스의 `judging` 이 false 다.
 */
class LogJudgingBacklogTest {
    private val startedAt = Instant.parse("2026-11-26T03:00:00Z")
    private var now = startedAt
    private val stream = FakeLogStream()
    private val sink = FakeLogSink()
    private val collector = LogCollector(stream, sink, batchSize = 200, block = Duration.ZERO, clock = { now })
    private val gate = LogJudgingGate(startedAt, Duration.ofMinutes(3), { collector.lastFailure }, { collector.lastLoopAt }, { collector.lastRoundCaughtUp })
    private val settings = LogAlertSettings(listOf(WatchedService("svc", "ci-agent-158", Duration.ofMinutes(3))), null, emptyList())

    /** 메모리 상태 저장 — 만든 알림을 모은다 */
    private class Store : LogRuleStateStore {
        var states: List<LogRuleState> = emptyList()
        val events = mutableListOf<ResourceAlert>()
        override fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome {
            val result = judge(states)
            states = result.states
            events += result.events
            return RoundOutcome(true, result.events.size)
        }
    }

    /** 저장된 줄에서 서비스마다의 마지막 시각 */
    private val receipts = object : LogReceipts {
        override fun lastReceived(services: Collection<String>) =
            services.associateWith { service -> sink.stored.values.filter { it.service == service }.maxOfOrNull { it.id.millis }?.let(Instant::ofEpochMilli) }
        override fun lastReceivedAny() = sink.stored.values.maxOfOrNull { it.id.millis }?.let(Instant::ofEpochMilli)
        override fun silentServers() = emptySet<String>()
    }

    private val store = Store()
    private val round = LogAlertRound(settings, store, receipts, LogSilentJudge(settings)) { gate.active(it) }
    private var sequence = 0

    private fun line(service: String, at: Instant) = stream.add(StreamRecord("${at.toEpochMilli()}-${sequence++}", "info", "req-$sequence:$service:hello"))

    @Test
    fun `수집이 끊겼다 돌아온 첫 바퀴가 상한만큼 읽으면 판정하지 않고 다 따라잡은 뒤 판정해 계속 보내던 서비스의 알림은 없다`() {
        now = startedAt.plus(Duration.ofMinutes(5))
        line("svc", now)
        collector.runOnce()
        assertTrue(collector.lastRoundCaughtUp)
        now = now.plusSeconds(30)
        assertTrue(round.judgeOnce(now).judged)

        // 수집이 끊긴다 — 그동안 다른 서비스 3,000건, 그 뒤에 svc 의 새 줄(돌아오기 5초 전)
        sink.failing = true
        val back = startedAt.plus(Duration.ofMinutes(9))
        repeat(3_000) { line("other", startedAt.plus(Duration.ofMinutes(6)).plusMillis(it.toLong())) }
        line("svc", back.minusSeconds(5))
        now = startedAt.plus(Duration.ofMinutes(6))
        collector.runOnce()
        assertNotNull(collector.lastFailure)
        assertEquals(0, round.judgeOnce(now).eventsCreated, "수집이 실패 중인데 알렸다")

        // 돌아온 첫 성공 바퀴 — 밀린 것을 상한만큼 읽었다(svc 의 새 줄은 아직 저장되지 않았다)
        sink.failing = false
        now = back
        collector.runOnce()
        assertNull(collector.lastFailure)
        assertFalse(collector.lastRoundCaughtUp)
        assertEquals(startedAt.plus(Duration.ofMinutes(5)), receipts.lastReceived(listOf("svc"))["svc"], "가짜 저장소가 시나리오대로가 아니다")
        val held = round.judgeOnce(now)
        assertEquals(0, held.eventsCreated, "다시 밀린 동안 판정해 계속 보내던 서비스를 넘음으로 알렸다")
        assertEquals(false, round.health(now)["judging"])

        // 다 따라잡는다
        var rounds = 0
        while (!collector.lastRoundCaughtUp && rounds++ < 100) { now = now.plusMillis(100); collector.runOnce() }
        assertTrue(collector.lastRoundCaughtUp)
        assertTrue(round.judgeOnce(now).judged, "다 따라잡았는데 판정하지 않았다")
        assertEquals(emptyList<ResourceAlert>(), store.events, "받고 있던 서비스에 넘음 · 해소 알림이 나갔다")
        assertFalse(store.states.single { it.service == "svc" }.firing)
    }

    @Test
    fun `로그가 끊임없이 와도 한 바퀴의 상한보다 적게 읽는 수집이면 빈 읽기가 없어도 판정한다`() {
        now = startedAt.plus(Duration.ofMinutes(4))
        repeat(20) { step ->
            repeat(150) { line(if (it == 0) "svc" else "other", now) }
            collector.runOnce()
            now = now.plusSeconds(30)
            assertTrue(collector.lastRoundCaughtUp, "${step + 1}번째: 상한보다 적게 읽었는데 따라잡지 않았다")
        }
        assertNull(collector.caughtUpAt, "시나리오대로가 아니다 — 빈 읽기가 있었다")
        assertTrue(gate.active(now), "끊임없이 오지만 따라잡고 있는 수집에서 판정하지 않았다")
        assertTrue(round.judgeOnce(now).judged)
        assertEquals(emptyList<ResourceAlert>(), store.events)
    }

    @Test
    fun `뜬 뒤 15분이 지나도 상한만큼씩만 읽으면 판정하지 않고 헬스의 judging 이 false 다`() {
        repeat(100) { line("other", startedAt.plusMillis(it.toLong())) }
        now = startedAt
        repeat(40) { step ->
            repeat(200) { line("other", now) }
            collector.runOnce()
            now = now.plusSeconds(30)
        }
        assertTrue(now >= startedAt.plus(Duration.ofMinutes(15)))
        assertFalse(collector.lastRoundCaughtUp)
        assertFalse(gate.active(now), "따라잡지 못한 채 15분이 지나자 판정했다")
        assertEquals(0, round.judgeOnce(now).eventsCreated)
        assertEquals(false, round.health(now)["judging"])
        assertEquals(emptyList<ResourceAlert>(), store.events, "밀린 동안 받은 기록 없음으로 알렸다")
    }
}
