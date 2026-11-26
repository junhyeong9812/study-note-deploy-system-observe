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
 * 무수신 판정의 보류 — 자원 끊김의 시작 조건([SilentGate]: 뜬 지 3분 · 로그 수집이 뜬 뒤 한 번 따라잡음 · 따라잡지 못해도 15분 뒤)을 그대로 쓰고,
 * 거기에 로그 수집이 지금 실패 중이거나 마지막 바퀴가 60초(같으면 지난 것) 넘게 끝나지 않았으면 보류한다(수집이 멈춘 동안의 공백은 로그가 없는 것이 아니다).
 * 로그 수집의 "따라잡음"([LogCollector.caughtUpAt]) = 뜬 뒤 처음으로 새 항목을 읽었는데 하나도 없던 바퀴가 끝난 시각(지표 수집과 같은 뜻 — 그 뒤로 바뀌지 않는다).
 */
class LogJudgingGateTest {
    private val startedAt = Instant.parse("2026-11-26T03:00:00Z")
    private var caughtUpAt: Instant? = startedAt
    private var failure: String? = null
    private var lastLoopAt: Instant? = null
    private val gate = LogJudgingGate(SilentGate(startedAt, { caughtUpAt }), { failure }, { lastLoopAt })

    private fun at(minutes: Long, seconds: Long = 0) = startedAt.plus(Duration.ofMinutes(minutes)).plusSeconds(seconds)

    @Test
    fun `뜬 직후 3분과 로그 수집이 따라잡기 전에는 판정하지 않고 따라잡지 못해도 15분 뒤에는 판정한다`() {
        lastLoopAt = at(2, 59)
        assertFalse(gate.active(at(2, 59)), "뜬 직후에 판정했다")
        lastLoopAt = at(3)
        assertTrue(gate.active(at(3)))
        caughtUpAt = null
        lastLoopAt = at(10)
        assertFalse(gate.active(at(10)), "로그 수집이 따라잡기 전에 판정했다")
        lastLoopAt = at(15)
        assertTrue(gate.active(at(15)), "따라잡지 못한 채 영영 미뤘다")
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
