package xyz.junproject.observe.logalert

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.StreamRecord
import xyz.junproject.observe.threshold.SilentGate
import java.time.Duration
import java.time.Instant

/**
 * 무수신 판정의 보류(§16) — 뜬 지 3분(같으면 지난 것) · 로그 수집이 실패 중이 아님 · 마지막 바퀴가 60초(같으면 지난 것) 안 ·
 * **마지막으로 성공한 수집 바퀴가 따라잡은 바퀴**([LogCollector.lastRoundCaughtUp] — 새로 읽은 수가 한 바퀴의 상한보다 적었다, 빈 읽기 포함)일 때만 판정한다.
 * 따라잡지 못한 채 15분이 지나도 판정하지 않는다(자원 끊김의 [SilentGate] 와 달리 포기하지 않는다 — 밀린 동안의 판정은 거짓 알림이다).
 * 로그 수집의 "뜬 뒤 처음 따라잡음"([LogCollector.caughtUpAt] — 새 항목이 하나도 없던 첫 바퀴)은 그대로 남는다.
 */
class LogJudgingGateTest {
    private val startedAt = Instant.parse("2026-11-26T03:00:00Z")
    private var caughtUp = true
    private var failure: String? = null
    private var lastLoopAt: Instant? = null
    private val gate = LogJudgingGate(startedAt, Duration.ofMinutes(3), { failure }, { lastLoopAt }, { caughtUp })

    private fun at(minutes: Long, seconds: Long = 0) = startedAt.plus(Duration.ofMinutes(minutes)).plusSeconds(seconds)

    @Test
    fun `뜬 직후 3분에는 판정하지 않고 마지막 바퀴가 따라잡은 바퀴일 때만 판정하며 15분이 지나도 따라잡지 못했으면 판정하지 않는다`() {
        lastLoopAt = at(2, 59)
        assertFalse(gate.active(at(2, 59)), "뜬 직후에 판정했다")
        lastLoopAt = at(3)
        assertTrue(gate.active(at(3)))
        caughtUp = false
        lastLoopAt = at(10)
        assertFalse(gate.active(at(10)), "마지막 바퀴가 따라잡지 못했는데 판정했다")
        lastLoopAt = at(15)
        assertFalse(gate.active(at(15)), "15분이 지나자 따라잡지 못한 채 판정했다")
        lastLoopAt = at(60)
        assertFalse(gate.active(at(60)))
        caughtUp = true
        assertTrue(gate.active(at(60)), "다시 따라잡았는데 판정하지 않았다")
    }

    @Test
    fun `로그 수집이 실패 중이거나 마지막 바퀴가 60초 넘게 오래면 판정하지 않는다`() {
        lastLoopAt = at(10)
        assertTrue(gate.active(at(10, 59)))
        assertFalse(gate.active(at(11)), "수집이 60초 동안 바퀴를 끝내지 못했는데 판정했다")
        failure = "QueryTimeoutException"
        assertFalse(gate.active(at(10, 1)), "수집이 실패 중인데 판정했다")
        failure = null
        lastLoopAt = null
        assertFalse(gate.active(at(20)), "수집이 한 바퀴도 끝내지 못했는데 판정했다")
    }

    @Test
    fun `로그 수집은 성공한 바퀴마다 새로 읽은 수가 한 바퀴의 상한보다 적었는가를 따라잡음으로 남기고 실패한 바퀴는 그 값을 바꾸지 않는다`() {
        val now = startedAt
        val stream = FakeLogStream()
        val sink = FakeLogSink()
        val collector = LogCollector(stream, sink, batchSize = 3, block = Duration.ZERO, clock = { now })
        assertFalse(collector.lastRoundCaughtUp, "한 바퀴도 돌지 않았는데 따라잡았다")
        fun add(count: Int, from: Int) = repeat(count) { stream.add(StreamRecord("${1000 + from + it}-0", "info", "req-$it:backend:hello")) }

        add(2, 0); collector.runOnce()
        assertTrue(collector.lastRoundCaughtUp, "상한보다 적게 읽은 바퀴인데 따라잡지 않았다")
        collector.runOnce()
        assertTrue(collector.lastRoundCaughtUp, "빈 읽기는 따라잡은 바퀴다")
        add(3, 10); collector.runOnce()
        assertFalse(collector.lastRoundCaughtUp, "상한만큼 읽은 바퀴인데 따라잡았다")
        collector.runOnce()
        assertTrue(collector.lastRoundCaughtUp)

        add(8, 20)
        sink.failing = true
        collector.runOnce()
        assertNotNull(collector.lastFailure)
        assertTrue(collector.lastRoundCaughtUp, "실패한 바퀴가 따라잡음을 바꿨다")
        sink.failing = false
        collector.runOnce()
        assertFalse(collector.lastRoundCaughtUp, "끊겼다 돌아온 첫 바퀴가 새로 상한만큼 읽었는데 따라잡았다")
        collector.runOnce()
        assertTrue(collector.lastRoundCaughtUp, "남은 것을 상한보다 적게 읽은 바퀴인데 따라잡지 않았다")
        assertEquals(13, sink.stored.size, "밀린 줄을 다 저장하지 않았다")
    }

    @Test
    fun `로그 수집은 뜬 뒤 처음으로 새 항목이 없던 바퀴의 시각을 따라잡은 시각으로 남기고 그 뒤로 바꾸지 않는다`() {
        var now = startedAt
        val stream = FakeLogStream()
        val collector = LogCollector(stream, FakeLogSink(), block = Duration.ZERO, clock = { now })
        stream.add(StreamRecord("1000-0", "info", "req-1:backend:hello"))
        collector.runOnce()
        assertNull(collector.caughtUpAt, "밀린 것을 읽은 바퀴인데 따라잡았다고 남겼다")
        now = startedAt.plusSeconds(5)
        collector.runOnce()
        assertEquals(startedAt.plusSeconds(5), collector.caughtUpAt)
        now = startedAt.plusSeconds(10)
        stream.add(StreamRecord("2000-0", "info", "req-2:backend:hello"))
        collector.runOnce()
        collector.runOnce()
        assertEquals(startedAt.plusSeconds(5), collector.caughtUpAt, "따라잡은 시각이 바뀌었다")
    }
}
