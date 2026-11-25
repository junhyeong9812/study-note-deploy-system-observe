package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/** 명세 §15 · §15.1 (리뷰 루프 1) — 측정 시각이 수신 시각보다 6일 넘게 뒤인 항목은 `too_old` 로 버린다(정확히 6일은 받는다) */
class TooOldEntryTest {
    private val receivedAt = Instant.parse("2026-11-24T03:00:00Z")

    @Test
    fun `6일을 넘게 늦은 항목은 too_old 로 버리고 정확히 6일은 받는다`() {
        val sixDays = receivedAt.minus(Duration.ofDays(6))
        assertTrue(MetricEntryParser.parse(MetricRecords.resource("1-0", sixDays), receivedAt) is MetricEntryParse.Accepted)
        assertEquals(MetricEntryParse.Dropped(MetricDropReason.TOO_OLD),
            MetricEntryParser.parse(MetricRecords.resource("2-0", sixDays.minusMillis(1)), receivedAt))
        assertEquals("too_old", MetricDropReason.TOO_OLD.wireName)
    }

    @Test
    fun `수집기가 too_old 를 세고 확인하며 헬스의 dropped 에 그 칸이 나온다`() {
        val stream = FakeMetricStream()
        val collector = MetricCollector(stream, FakeMetricSink(), block = Duration.ZERO, clock = { receivedAt })
        stream.add(MetricRecords.resource("1-0", receivedAt.minus(Duration.ofDays(8))))
        collector.runOnce()
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.TOO_OLD])
        assertTrue(stream.pending.isEmpty())
        val body = MetricHealth(collector, { 0L }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null }, startedAt = receivedAt).report(receivedAt)
        assertEquals(1L, ((body["dropped"] as Map<*, *>)["too_old"] as Number).toLong())
    }
}
