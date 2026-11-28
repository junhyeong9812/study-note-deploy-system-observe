package xyz.junproject.observe.traffic

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.MetricStreamRecord
import java.time.Duration
import java.time.Instant

/**
 * 규약 4.5 · 4.1(지표와 같은 수집 코드) — 저장한 뒤에만 확인, 깨진 항목은 이유를 세고 확인, 저장 실패는 확인하지 않음(다시 받는다),
 * 순번으로 버린 항목 · 칸마다 버린 수 · `at` 의 앞섬 · 오래됨을 센다(항목은 버리지 않는다). 로그에 항목의 내용 · 예외 문구를 싣지 않는다.
 */
class TrafficCollectorTest {
    private val events = mutableListOf<String>()
    private val stream = FakeMetricStream(events)
    private val sink = FakeTrafficSink(events)
    private var now = Instant.parse("2026-11-25T04:00:00Z")
    private val collector = TrafficCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })
    private val logs = ListAppender<ILoggingEvent>()
    private val logger = LoggerFactory.getLogger("observe") as Logger

    @BeforeEach fun captureLogs() { logs.start(); logger.addAppender(logs) }
    @AfterEach fun releaseLogs() { logger.detachAppender(logs) }

    private fun good(millis: Long, sequence: Long, at: Instant = now.minusSeconds(1), body: String = TrafficRecords.body()) =
        TrafficRecords.record("$millis-0", sequence, at, body)
    private fun broken(millis: Long, sequence: Long) = good(millis, sequence).let { it.copy(fields = it.fields + ("body" to "{")) }
    private fun logText() = logs.list.joinToString("\n") { it.formattedMessage + (it.throwableProxy?.message ?: "") }

    @Test
    fun `저장한 뒤에 확인하고 받은 시각은 수집기의 시계다`() {
        stream.add(good(1_000, 1), good(2_000, 2))
        assertEquals(2, collector.runOnce())
        assertEquals(listOf("store:1", "ack:1000-0", "store:2", "ack:2000-0"), events)
        assertTrue(sink.stored.values.all { it.receivedAt == now })
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
        assertEquals(2L, collector.storedTotal)
        assertEquals(now, collector.lastLoopAt)
    }

    @Test
    fun `깨진 항목은 이유를 세고 확인하고 같은 묶음의 다른 항목은 저장한다`() {
        stream.add(good(1_000, 1), broken(2_000, 2), good(3_000, 3))
        assertEquals(3, collector.runOnce())
        assertEquals(setOf(1L, 3L), sink.stored.keys.map { it.third }.toSet())
        assertTrue(stream.pending.isEmpty(), "깨진 항목이 미확인으로 남았다")
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.BODY_NOT_JSON])
        assertEquals(TrafficDropReason.entries.toSet(), collector.droppedCounts().keys, "모든 이유가 0 부터 들어 있지 않다")
    }

    @Test
    fun `저장이 실패하면 그 항목과 뒤의 항목은 확인하지 않고 다음 바퀴에 다시 받아 저장한다`() {
        sink.failing = { it.sequence == 2L }
        stream.add(good(1_000, 1), good(2_000, 2), good(3_000, 3))
        assertEquals(0, collector.runOnce())
        assertEquals(listOf("1000-0"), stream.acknowledged, "저장하지 못한 항목을 확인했다")
        assertEquals(setOf("2000-0", "3000-0"), stream.pending.keys)
        assertEquals("IllegalStateException", collector.lastFailure)
        assertEquals("IllegalStateException", collector.lastStoreFailure)
        assertEquals(now, collector.lastStoreFailureAt)
        assertEquals(0L, collector.droppedCounts().values.sum(), "저장 실패를 깨진 항목으로 셌다")

        sink.failing = { false }
        now = now.plusSeconds(10)
        collector.runOnce()
        assertTrue(stream.pending.isEmpty())
        assertEquals(setOf(1L, 2L, 3L), sink.stored.keys.map { it.third }.toSet())
        assertNull(collector.lastFailure)
        assertEquals("IllegalStateException", collector.lastStoreFailure, "마지막 저장 실패가 성공한 바퀴에 지워졌다")
        assertEquals(now.minusSeconds(10), collector.lastStoreFailureAt)
    }

    @Test
    fun `이미 받은 순번은 duplicates 로 세고 확인하며 저장한 수에 넣지 않는다`() {
        stream.add(good(1_000, 1), good(2_000, 1))
        collector.runOnce()
        assertEquals(1L, collector.duplicates)
        assertEquals(1L, collector.storedTotal)
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `저장이 칸마다 버린 수를 이유마다 센다`() {
        sink.outcome = { TrafficStoreOutcome(false, segmentsStored = 1, segmentsAhead = 2, segmentsTooOld = 3, requestsStored = 1, requestsAhead = 4, requestsTooOld = 5) }
        stream.add(good(1_000, 1))
        collector.runOnce()
        assertEquals(
            mapOf(TrafficCellDrop.SEGMENT_AHEAD to 2L, TrafficCellDrop.SEGMENT_TOO_OLD to 3L, TrafficCellDrop.REQUEST_AHEAD to 4L, TrafficCellDrop.REQUEST_TOO_OLD to 5L),
            collector.cellDropCounts(),
        )
        assertEquals(listOf("segment_ahead", "segment_too_old", "request_ahead", "request_too_old"), TrafficCellDrop.entries.map { it.wireName })
    }

    @Test
    fun `at 이 2분 넘게 앞서거나 6일 넘게 뒤진 항목도 저장하고 그 수만 센다 — 다시 온 같은 순번은 세지 않는다`() {
        stream.add(
            good(1_000, 1, at = now.plus(Duration.ofMinutes(2)).plusMillis(1)),
            good(2_000, 2, at = now.plus(Duration.ofMinutes(2))),
            good(3_000, 3, at = now.minus(Duration.ofDays(6)).minusMillis(1)),
            good(4_000, 4, at = now.minus(Duration.ofDays(6))),
            good(5_000, 1, at = now.plus(Duration.ofMinutes(2)).plusMillis(1)),
        )
        collector.runOnce()
        assertEquals(setOf(1L, 2L, 3L, 4L), sink.stored.keys.map { it.third }.toSet(), "at 으로 항목을 버렸다")
        assertEquals(1L, collector.atAhead)
        assertEquals(1L, collector.atTooOld)
    }

    @Test
    fun `그 줄만 버린 요청별 줄의 수를 새로 받은 항목에서만 센다`() {
        val body = TrafficRecords.body(requests = listOf(TrafficRecords.request(now.minusSeconds(2), path = "/a?b"), TrafficRecords.request(now.minusSeconds(2), path = "/ip/10.0.0.1"),
            TrafficRecords.request(now.minusSeconds(2))))
        stream.add(good(1_000, 1, body = body), good(2_000, 1, body = body))
        collector.runOnce()
        assertEquals(2L, collector.requestsRejected)
        assertEquals(1, sink.stored.values.single().requests.size)
    }

    @Test
    fun `한 칸의 수가 10억을 넘는 항목은 꼴 틀림으로 세고 확인하며 같은 묶음의 다른 항목은 저장한다`() {
        val huge = 1_000_000_001L
        val overflowing = TrafficRecords.body(segments = listOf("""{"bucket_start_ms": ${TrafficRecords.cellStart(now).toEpochMilli()}, "service": "backend", "caller": "front",
            "class": "request", "count": $huge, "status": {"2xx": $huge, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0}, "buckets": [$huge${", 0".repeat(19)}], "max_ms": 3}"""))
        stream.add(good(1_000, 1, body = overflowing), good(2_000, 2))
        collector.runOnce()
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.INVALID_BODY])
        assertTrue(stream.pending.isEmpty(), "넘친 항목을 확인하지 않아 영영 다시 받는다")
        assertEquals(listOf(2L), sink.stored.keys.map { it.third })
    }

    @Test
    fun `requests_dropped 나 상태 값이 10억을 넘는 항목은 꼴 틀림으로 세고 확인하며 다른 항목은 저장한다`() {
        val cellStart = TrafficRecords.cellStart(now).toEpochMilli()
        val wrappedStatus = TrafficRecords.body(segments = listOf("""{"bucket_start_ms": $cellStart, "service": "backend", "caller": "front", "class": "request",
            "count": 1000000000, "status": {"2xx": ${Long.MAX_VALUE}, "3xx": ${Long.MAX_VALUE}, "4xx": 1000000002, "5xx": 0, "other": 0},
            "buckets": [1000000000${", 0".repeat(19)}], "max_ms": 3}"""))
        stream.add(good(1_000, 1, body = TrafficRecords.body(requestsDropped = 6_000_000_000_000_000_000)), good(2_000, 2, body = wrappedStatus), good(3_000, 3))
        collector.runOnce()
        assertEquals(2L, collector.droppedCounts()[TrafficDropReason.INVALID_BODY])
        assertTrue(stream.pending.isEmpty(), "버린 항목을 확인하지 않아 영영 다시 받는다")
        assertEquals(listOf(3L), sink.stored.keys.map { it.third })
    }

    @Test
    fun `다른 종류는 저장하지 않고 건너뛴 수를 세고 확인한다`() {
        stream.add(MetricStreamRecord("1000-0", mapOf("kind" to "resource", "v" to "1")), good(2_000, 1))
        collector.runOnce()
        assertEquals(1L, collector.skippedOtherKind)
        assertTrue(stream.pending.isEmpty())
        assertEquals(1, sink.stored.size)
    }

    @Test
    fun `로그에는 항목의 내용과 예외 문구를 싣지 않는다 — 깨진 항목 · 저장 실패 모두`() {
        val marked = TrafficRecords.body(requests = listOf(TrafficRecords.request(now.minusSeconds(2), path = "/MARKER-path", requestId = "MARKER-id")))
        sink.failing = { true }
        stream.add(
            good(1_000, 1, body = marked),
            MetricStreamRecord("2000-0", TrafficRecords.fields(2, now, marked.replace("\"dependencies\": []", "\"dependencies\": \"MARKER-broken\""))),
        )
        collector.runOnce()
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.INVALID_BODY])
        assertNotNull(collector.lastFailure)
        val text = logText()
        assertTrue(text.contains("2000-0"), "버린 항목의 스트림 ID 가 로그에 없다: $text")
        assertTrue(text.contains("invalid_body"), "버린 이유가 로그에 없다: $text")
        assertFalse(text.contains("MARKER"), "항목의 내용 · 예외 문구가 로그에 실렸다: $text")
    }

    @Test
    fun `스키마를 만들지 못하면 실패로 남고 다음 바퀴에 다시 만든다`() {
        val failingSchema = object : TrafficSink by sink {
            var fail = true
            override fun createSchema() { if (fail) throw IllegalStateException("permission denied") else sink.createSchema() }
        }
        val schemaCollector = TrafficCollector(stream, failingSchema, batchSize = 10, block = Duration.ZERO, clock = { now })
        stream.add(good(1_000, 1))
        assertEquals(0, schemaCollector.runOnce())
        assertEquals("schema IllegalStateException", schemaCollector.lastFailure)
        assertTrue(stream.acknowledged.isEmpty(), "스키마 없이 확인했다")
        failingSchema.fail = false
        schemaCollector.runOnce()
        assertEquals(1, sink.stored.size)
        assertNull(schemaCollector.lastFailure)
    }
}
