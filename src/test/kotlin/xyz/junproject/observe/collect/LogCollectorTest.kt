package xyz.junproject.observe.collect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/** 명세 ②: 저장에 성공한 뒤에만 확인(ack)한다 — 실패한 묶음은 미확인으로 남아 다음 바퀴에 다시 받는다 */
class LogCollectorTest {
    private val events = mutableListOf<String>()
    private val stream = FakeLogStream(events)
    private val sink = FakeLogSink(events)
    private var now = Instant.parse("2026-11-21T00:00:00Z")
    private val collector = LogCollector(stream, sink, batchSize = 2, block = Duration.ZERO, clock = { now })

    private fun record(millis: Long, sequence: Int = 0, line: String = "req-$millis:backend:message $millis") =
        StreamRecord("$millis-$sequence", "info", line)

    @Test
    fun `저장한 뒤에 확인하고 확인한 것만 미확인에서 빠진다`() {
        stream.add(record(1_000), record(2_000))
        assertEquals(2, collector.runOnce())
        assertEquals(listOf("insert:2", "ack:2"), events)
        assertEquals(setOf(StreamId(1_000, 0), StreamId(2_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
    }

    @Test
    fun `저장에 실패하면 확인하지 않고 다음 바퀴에 같은 항목을 다시 받아 저장한다`() {
        stream.add(record(1_000), record(2_000))
        sink.failing = true
        assertEquals(0, collector.runOnce())
        assertFalse(events.any { it.startsWith("ack") }, "저장에 실패했는데 확인했다: $events")
        assertEquals(setOf("1000-0", "2000-0"), stream.pending.keys)
        assertNotNull(collector.lastFailure)

        // 다음 바퀴 — 시계가 그대로여도(1분 주기를 기다리지 않고) 미확인분부터 다시 받는다
        sink.failing = false
        assertEquals(2, collector.runOnce())
        assertEquals(setOf(StreamId(1_000, 0), StreamId(2_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
    }

    @Test
    fun `묶음 중간에 실패해도 앞 묶음만 확인되고 남은 것은 미확인으로 남는다`() {
        stream.add(record(1_000), record(2_000), record(3_000), record(4_000))
        collector.runOnce()                                    // batchSize 2 → 두 건만 새로 읽는다
        assertEquals(setOf("1000-0", "2000-0"), stream.acknowledged.toSet())
        sink.failing = true
        collector.runOnce()
        assertEquals(setOf("3000-0", "4000-0"), stream.pending.keys)
        assertEquals(2, stream.acknowledged.size)
    }

    @Test
    fun `기동 직후 지난번에 확인하지 못한 항목부터 받는다`() {
        stream.add(record(1_000), record(2_000), record(3_000))
        stream.ensureGroup()
        stream.readNew(10, Duration.ZERO)                    // 앞 프로세스가 읽고 저장하기 전에 죽었다
        stream.add(record(4_000))
        val restarted = LogCollector(stream, sink, batchSize = 2, block = Duration.ZERO, clock = { now })
        assertEquals(4, restarted.runOnce())
        assertEquals(4, sink.stored.size)
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `Redis 에 닿지 못하면 실패로 남기고 이어지면 다시 받는다`() {
        stream.add(record(1_000))
        stream.reachable = false
        assertEquals(0, collector.runOnce())
        assertNotNull(collector.lastFailure)
        stream.reachable = true
        assertEquals(1, collector.runOnce())
        assertNull(collector.lastFailure)
    }

    @Test
    fun `Redis 가 재시작해 그룹이 사라져도 다음 바퀴에 그룹을 다시 만들고 이어 받는다`() {
        stream.add(record(1_000))
        assertEquals(1, collector.runOnce())
        stream.restartWithoutPersistence()                   // 명세 ②: 재시작 때 남아 있던 미수집분만 잃는다
        stream.add(record(5_000))                           // 발신 쪽이 다시 보내 스트림이 새로 생겼다
        collector.runOnce()                                 // 첫 바퀴는 그룹이 없어 실패할 수 있다
        collector.runOnce()
        assertTrue(StreamId(5_000, 0) in sink.stored, "재시작 뒤의 로그를 받지 못했다 — 수집이 멈췄다")
        assertNull(collector.lastFailure)
    }

    @Test
    fun `내용에 콜론·줄바꿈이 있어도 그 항목과 묶음이 함께 저장된다`() {
        stream.add(record(1_000, line = "req-1:backend:a:b:c\nd"), record(1_000, 1, line = ""), record(2_000, line = "x".repeat(100_000)))
        val bigBatch = LogCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })
        assertEquals(3, bigBatch.runOnce())
        assertEquals("a:b:c\nd", sink.stored[StreamId(1_000, 0)]!!.message)
        assertEquals("unknown", sink.stored[StreamId(1_000, 1)]!!.service)
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `상한에 밀려 필드가 사라진 미확인 항목은 빈 행으로 저장하지 않고 확인만 한다`() {
        // Redis 는 미확인인 채로 지워진 항목을 ID 와 빈 필드로 돌려준다 — 로그 줄이 아니다
        stream.add(StreamRecord("1000-0", null, null), StreamRecord("2000-0", "info", null))
        assertEquals(2, collector.runOnce())
        assertEquals(setOf(StreamId(2_000, 0)), sink.stored.keys, "line 만 없는 항목은 남기고, 필드가 다 사라진 항목은 남기지 않는다")
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `저장한 건수를 센다`() {
        // 마지막 저장 시각은 헬스가 DB 에서 읽는다(재시작 직후에도 보이게) — 수집기는 이 프로세스가 저장한 건수만 센다
        stream.add(record(1_763_700_000_123), record(1_763_700_000_999))
        collector.runOnce()
        assertEquals(2, collector.storedTotal)
    }

    @Test
    fun `스키마를 만들지 못하면 그 바퀴는 읽지도 확인하지도 않고 실패 사유에 schema 를 남긴다`() {
        stream.add(record(1_000))
        sink.schemaFailing = true
        assertEquals(0, collector.runOnce())
        assertEquals("schema IllegalStateException", collector.lastFailure)
        assertNull(collector.lastLoopAt, "스키마 없이 바퀴가 성공한 것으로 보였다")
        assertFalse(events.any { it.startsWith("ack") || it.startsWith("insert") }, "$events")

        sink.schemaFailing = false
        assertEquals(1, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(1, sink.schemaCreations)
        collector.runOnce()
        assertEquals(1, sink.schemaCreations, "성공한 뒤에도 바퀴마다 스키마를 다시 만든다")
    }
}
