package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/**
 * 명세 §2.3 · §11.2 — 저장 뒤에만 확인, 깨진 항목은 이유를 세고 확인(다시 받지 않음), DB 실패는 확인하지 않음(다음 바퀴에 다시),
 * 깨진 항목 하나가 묶음의 다른 항목을 막지 않음, 다른 종류는 건너뛰고 확인. 수신 시각은 수집기의 시계.
 */
class MetricCollectorTest {
    private val events = mutableListOf<String>()
    private val stream = FakeMetricStream(events)
    private val sink = FakeMetricSink(events)
    private var now = Instant.parse("2026-11-24T03:00:00Z")
    private val collector = MetricCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })

    private fun good(millis: Long, measuredAt: Instant = now.minusSeconds(1)) = MetricRecords.resource("$millis-0", measuredAt)
    private fun broken(millis: Long) = MetricRecords.resource("$millis-0", now).let { it.copy(fields = it.fields + ("body" to "{")) }
    private fun otherKind(millis: Long) = MetricStreamRecord("$millis-0", mapOf("kind" to "trace", "payload" to "x"))

    @Test
    fun `저장한 뒤에 확인하고 수신 시각은 수집기의 시계다`() {
        stream.add(good(1_000), good(2_000))
        assertEquals(2, collector.runOnce())
        assertEquals(listOf("insert:1000-0", "insert:2000-0", "ack:1000-0", "ack:2000-0"), events)
        assertEquals(setOf(StreamId(1_000, 0), StreamId(2_000, 0)), sink.stored.keys)
        assertTrue(sink.stored.values.all { it.receivedAt == now })
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
        assertEquals(now, collector.lastLoopAt)
        assertEquals(2L, collector.storedTotal)
    }

    @Test
    fun `깨진 항목 하나가 같은 묶음의 다른 항목 저장을 막지 않고 이유를 세고 확인한다`() {
        stream.add(good(1_000), broken(2_000), good(3_000))
        assertEquals(3, collector.runOnce())
        assertEquals(setOf(StreamId(1_000, 0), StreamId(3_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty(), "깨진 항목이 미확인으로 남았다 — 영영 다시 받는다")
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.BODY_NOT_JSON])
        assertNull(collector.lastFailure, "깨진 항목은 바퀴의 실패가 아니다")
    }

    @Test
    fun `버린 건수는 이유마다 세고 모든 이유가 0 부터 들어 있다`() {
        assertEquals(MetricDropReason.entries.toSet(), collector.droppedCounts().keys)
        assertTrue(collector.droppedCounts().values.all { it == 0L })
        stream.add(broken(1_000), broken(2_000), MetricStreamRecord("3000-0", emptyMap()), good(4_000).let { it.copy(fields = it.fields + ("v" to "2")) })
        collector.runOnce()
        val counts = collector.droppedCounts()
        assertEquals(2L, counts[MetricDropReason.BODY_NOT_JSON])
        assertEquals(1L, counts[MetricDropReason.TRIMMED])
        assertEquals(1L, counts[MetricDropReason.UNSUPPORTED_VERSION])
        assertEquals(4L, counts.values.sum())
    }

    @Test
    fun `측정 시각이 수집기의 시계보다 2분 넘게 앞서면 버리고 clock_ahead 로 센다`() {
        stream.add(good(1_000, measuredAt = now.plus(Duration.ofMinutes(2))), good(2_000, measuredAt = now.plus(Duration.ofMinutes(2)).plusMillis(1)))
        collector.runOnce()
        assertEquals(setOf(StreamId(1_000, 0)), sink.stored.keys)
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.CLOCK_AHEAD])
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `다른 종류는 저장하지 않고 버린 것으로도 세지 않고 건너뛴 수를 세고 확인한다`() {
        stream.add(otherKind(1_000), good(2_000))
        assertEquals(2, collector.runOnce())
        assertEquals(setOf(StreamId(2_000, 0)), sink.stored.keys)
        assertEquals(1L, collector.skippedOtherKind)
        assertEquals(0L, collector.droppedCounts().values.sum())
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `저장에 실패하면 받아들인 항목은 확인하지 않고 다음 바퀴에 다시 받아 저장하며 깨진 항목은 한 번만 센다`() {
        stream.add(good(1_000), broken(2_000), good(3_000))
        sink.failing = true
        assertEquals(0, collector.runOnce())
        assertEquals(setOf("1000-0", "3000-0"), stream.pending.keys, "저장에 실패한 항목이 확인됐거나 깨진 항목이 남았다")
        assertFalse(events.contains("ack:1000-0") || events.contains("ack:3000-0"), "$events")
        assertNotNull(collector.lastFailure)

        sink.failing = false
        collector.runOnce()                                   // 시계가 그대로여도 미확인분부터 다시 받는다
        assertEquals(setOf(StreamId(1_000, 0), StreamId(3_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.BODY_NOT_JSON], "다시 받아 두 번 셌다")
    }

    @Test
    fun `기동 직후 지난번에 확인하지 못한 항목부터 받고 같은 항목은 한 번만 저장된다`() {
        stream.add(good(1_000), good(2_000))
        stream.ensureGroup()
        stream.readNew(10, Duration.ZERO)                     // 앞 프로세스가 읽고 저장했지만 확인 전에 죽었다
        sink.insertAll(listOf(MetricEntryParser.parse(good(1_000), now).let { (it as MetricEntryParse.Accepted).sample }))
        stream.add(good(3_000))
        val restarted = MetricCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })
        assertEquals(3, restarted.runOnce())
        assertEquals(3, sink.stored.size)
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `Redis 에 닿지 못하면 실패로 남기고 이어지면 다시 받는다`() {
        stream.add(good(1_000))
        stream.reachable = false
        assertEquals(0, collector.runOnce())
        assertNotNull(collector.lastFailure)
        stream.reachable = true
        assertEquals(1, collector.runOnce())
        assertNull(collector.lastFailure)
    }

    @Test
    fun `스키마를 만들지 못하면 읽지도 확인하지도 않고 실패 사유에 schema 를 남긴다`() {
        stream.add(good(1_000))
        sink.schemaFailing = true
        assertEquals(0, collector.runOnce())
        assertEquals("schema IllegalStateException", collector.lastFailure)
        assertNull(collector.lastLoopAt)
        assertTrue(events.none { it.startsWith("ack") || it.startsWith("insert") }, "$events")
        sink.schemaFailing = false
        assertEquals(1, collector.runOnce())
        collector.runOnce()
        assertEquals(1, sink.schemaCreations, "성공한 뒤에도 바퀴마다 스키마를 다시 만든다")
    }

    @Test
    fun `실패 사유에는 예외 문구 없이 종류 이름만 남긴다`() {
        stream.add(good(1_000))
        sink.failing = true
        collector.runOnce()
        assertEquals("IllegalStateException", collector.lastFailure)
    }
}
