package xyz.junproject.observe.visit

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.SEARCH_WORDS
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import java.time.Duration
import java.time.Instant

/**
 * 방문 수집 한 바퀴 — 기존 수집기(로그 · 지표)와 같은 규칙: 저장한 뒤에만 확인 · 꼴이 틀린 항목은 그 항목만 버리고 이유를 세고 확인
 * (다시 받지 않는다 · 같은 묶음의 다른 항목은 저장) · 저장이 실패하면 확인하지 않고 다음 바퀴에 다시.
 * 그리고 주소 원문 · 검색어는 버린 항목의 로그에도, 저장 실패의 로그 · 실패 사유에도 나오지 않는다.
 */
class VisitCollectorTest {
    private val events = mutableListOf<String>()
    private val stream = FakeMetricStream(events)
    private val sink = FakeVisitSink(events)
    private val now = Instant.parse("2026-11-25T03:00:00Z")
    private val collector = VisitCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })
    private val logs = CapturedLogs()

    @AfterEach fun detach() = logs.close()

    private fun good(millis: Long, ip: String = ADDRESS_V4) = VisitRecords.record("$millis-0", VisitRecords.fields(ip = ip))
    private fun badAddress(millis: Long) = VisitRecords.record("$millis-0", VisitRecords.fields(ip = "$ADDRESS_V4:443"))

    @Test
    fun `저장한 뒤에 확인하고 한 항목은 한 줄이다`() {
        stream.add(good(1_000), good(2_000, ip = ADDRESS_V6))
        assertEquals(2, collector.runOnce())
        assertEquals(listOf("insert:1000-0", "insert:2000-0", "ack:1000-0", "ack:2000-0"), events)
        assertEquals(listOf(ADDRESS_V4, ADDRESS_V6), sink.stored.values.map { it.ip })
        assertTrue(stream.pending.isEmpty())
        assertEquals(2L, collector.storedTotal)
        assertEquals(now, collector.lastLoopAt)
        assertNull(collector.lastFailure)
    }

    @Test
    fun `꼴이 틀린 항목은 그 항목만 버리고 이유를 세고 확인해 다시 받지 않는다`() {
        stream.add(good(1_000), badAddress(2_000), VisitRecords.record("3000-0", VisitRecords.fields(device = "tablet")), good(4_000))
        assertEquals(4, collector.runOnce())
        assertEquals(setOf(StreamId(1_000, 0), StreamId(4_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty(), "버린 항목이 미확인으로 남았다 — 영영 다시 받는다: ${stream.pending.keys}")
        assertTrue("2000-0" in stream.acknowledged && "3000-0" in stream.acknowledged)
        assertEquals(1L, collector.droppedCounts()[VisitDropReason.INVALID_IP])
        assertEquals(1L, collector.droppedCounts()[VisitDropReason.INVALID_DEVICE])
        assertNull(collector.lastFailure, "버린 항목은 바퀴의 실패가 아니다")
        collector.runOnce()
        assertEquals(2L, collector.droppedCounts().values.sum(), "다시 받아 두 번 셌다")
    }

    @Test
    fun `버린 건수는 이유마다 세고 모든 이유가 0 부터 들어 있다`() {
        assertEquals(VisitDropReason.entries.toSet(), collector.droppedCounts().keys)
        assertTrue(collector.droppedCounts().values.all { it == 0L })
        stream.add(badAddress(1_000), badAddress(2_000), VisitRecords.record("3000-0", emptyMap()), VisitRecords.record("4000-0", VisitRecords.fields(version = "2")))
        collector.runOnce()
        val counts = collector.droppedCounts()
        assertEquals(2L, counts[VisitDropReason.INVALID_IP])
        assertEquals(1L, counts[VisitDropReason.TRIMMED])
        assertEquals(1L, counts[VisitDropReason.UNSUPPORTED_VERSION])
        assertEquals(4L, counts.values.sum())
    }

    @Test
    fun `쿼리를 잘라 낸 항목은 저장하고 잘라 낸 수를 센다`() {
        stream.add(VisitRecords.record("1000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search")))
        collector.runOnce()
        assertEquals("/search", sink.stored.values.single().path)
        assertEquals(1L, collector.queryStripped)
        assertEquals(0L, collector.droppedCounts().values.sum())
    }

    @Test
    fun `저장에 실패하면 받아들인 항목은 확인하지 않고 다음 바퀴에 다시 받아 저장하며 버린 항목과 잘라 낸 수는 한 번만 센다`() {
        stream.add(good(1_000), badAddress(2_000), VisitRecords.record("3000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS")))
        sink.failure = IllegalStateException("DB 에 닿지 못함")
        assertEquals(0, collector.runOnce())
        assertEquals(setOf("1000-0", "3000-0"), stream.pending.keys, "저장에 실패한 항목이 확인됐거나 버린 항목이 남았다")
        assertNotNull(collector.lastFailure)

        sink.failure = null
        collector.runOnce()
        assertEquals(setOf(StreamId(1_000, 0), StreamId(3_000, 0)), sink.stored.keys)
        assertTrue(stream.pending.isEmpty())
        assertNull(collector.lastFailure)
        assertEquals(1L, collector.droppedCounts()[VisitDropReason.INVALID_IP], "다시 받아 두 번 셌다")
        assertEquals(1L, collector.queryStripped, "다시 받아 잘라 낸 수를 두 번 셌다")
        assertEquals(2L, collector.storedTotal)
    }

    @Test
    fun `같은 항목을 두 번 처리하면 저장 수와 잘라 낸 수는 한 번만 세고 두 번째는 이미 있음으로 센다`() {
        val record = VisitRecords.record("1000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search"))
        stream.add(record)
        collector.runOnce()
        assertEquals(1L, collector.storedTotal)
        assertEquals(0L, collector.alreadyStored)
        stream.add(record)                                     // 같은 스트림 ID 가 다시 온다 — 표에는 이미 있다
        assertEquals(1, collector.runOnce())
        assertTrue(stream.pending.isEmpty(), "이미 있는 항목을 확인하지 않았다 — 영영 다시 받는다")
        assertEquals(1, sink.stored.size)
        assertEquals(1L, collector.storedTotal, "이미 있던 항목을 저장 수에 셌다")
        assertEquals(1L, collector.alreadyStored, "이미 있던 항목을 따로 세지 않았다")
        assertEquals(1L, collector.queryStripped, "이미 있던 항목의 잘라 냄을 또 셌다")
        assertNull(collector.lastFailure, "이미 있는 항목은 실패가 아니다")
    }

    @Test
    fun `저장소가 하나도 넣지 않았다고 하면 저장 수는 그대로이고 묶음 전부를 이미 있음으로 센다`() {
        sink.insertAll(listOf(VisitRecords.visit(1_000, path = "/search", screen = "search"), VisitRecords.visit(2_000)))
        stream.add(VisitRecords.record("1000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search")), good(2_000))
        assertEquals(2, collector.runOnce())
        assertEquals(0L, collector.storedTotal)
        assertEquals(2L, collector.alreadyStored)
        assertEquals(0L, collector.queryStripped)
        assertTrue(stream.pending.isEmpty())
    }

    @Test
    fun `저장소가 결과 모름을 돌려준 항목은 저장 수에도 이미 있음에도 잘라 낸 수에도 세지 않고 따로 세며 확인한다`() {
        sink.insertAll(listOf(VisitRecords.visit(3_000)))
        sink.resultUnknownFor = setOf(StreamId(1_000, 0), StreamId(3_000, 0))
        stream.add(VisitRecords.record("1000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search")), good(2_000), good(3_000))
        assertEquals(3, collector.runOnce())
        assertNull(collector.lastFailure, "결과 모름은 바퀴의 실패가 아니다")
        assertTrue(stream.pending.isEmpty(), "결과 모름인 항목을 확인하지 않았다 — 영영 다시 받는다")
        assertEquals(1L, collector.storedTotal, "결과 모름을 저장 수에 셌다")
        assertEquals(2L, collector.storeResultUnknown, "결과 모름을 따로 세지 않았다")
        assertEquals(0L, collector.alreadyStored, "결과 모름을 이미 있음으로 짐작했다")
        assertEquals(0L, collector.queryStripped, "결과 모름인 항목의 잘라 냄을 셌다")
    }

    @Test
    fun `기동 직후 지난번에 확인하지 못한 항목부터 받고 같은 항목은 한 줄만 남는다`() {
        stream.add(good(1_000), good(2_000))
        stream.ensureGroup()
        stream.readNew(10, Duration.ZERO)                     // 앞 프로세스가 읽고 저장했지만 확인 전에 죽었다
        sink.insertAll(listOf(VisitRecords.visit(1_000)))
        stream.add(good(3_000))
        val restarted = VisitCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })
        assertEquals(3, restarted.runOnce())
        assertEquals(setOf(StreamId(1_000, 0), StreamId(2_000, 0), StreamId(3_000, 0)), sink.stored.keys)
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
        assertTrue(events.none { it.startsWith("ack") || it.startsWith("insert") }, "$events")
        sink.schemaFailing = false
        assertEquals(1, collector.runOnce())
        collector.runOnce()
        assertEquals(1, sink.schemaCreations, "성공한 뒤에도 바퀴마다 스키마를 다시 만든다")
    }

    // ── 주소 원문 · 검색어가 새지 않는다 ─────────────────────

    @Test
    fun `버린 항목의 로그에는 항목 ID 와 이유만 있고 항목의 원문은 없다`() {
        stream.add(badAddress(1_000), VisitRecords.record("2000-0", VisitRecords.fields(path = "/search\u0000?q=$SEARCH_WORDS", ip = ADDRESS_V6)))
        collector.runOnce()
        val text = logs.text()
        assertTrue(text.contains("1000-0") && text.contains("invalid_ip"), "버린 항목이 로그에 드러나지 않는다(조용한 버림): $text")
        assertTrue(text.contains("2000-0") && text.contains("control_character"), text)
        for (secret in listOf(ADDRESS_V4, ADDRESS_V6, SEARCH_WORDS, VISITOR_ONE, "www.google.com")) {
            assertFalse(text.contains(secret), "로그에 항목의 원문($secret)이 나왔다: $text")
        }
    }

    @Test
    fun `저장 실패의 예외 글에 주소가 들어 있어도 로그와 실패 사유에는 나오지 않는다`() {
        // PostgreSQL 드라이버의 묶음 저장 오류는 문구에 넣은 값을 그대로 싣는다 — 그 꼴을 흉내 낸다
        sink.failure = IllegalStateException(
            "Batch entry 0 INSERT INTO visit_event VALUES (('1764000000000'::int8), ('$ADDRESS_V4'), ('$VISITOR_ONE')) was aborted: ERROR: boom",
            RuntimeException("ERROR: value ('$ADDRESS_V6') rejected"),
        )
        stream.add(good(1_000), good(2_000, ip = ADDRESS_V6))
        collector.runOnce()
        collector.runOnce()                                    // 같은 실패가 이어져도
        assertNotNull(collector.lastFailure)
        val text = logs.text()
        assertTrue(text.isNotBlank() && text.contains("IllegalStateException"), "저장 실패가 로그에 드러나지 않는다: $text")
        for (secret in listOf(ADDRESS_V4, ADDRESS_V6, VISITOR_ONE)) {
            assertFalse(text.contains(secret), "로그에 $secret 이 나왔다: $text")
            assertFalse(collector.lastFailure!!.contains(secret), "실패 사유에 $secret 이 나왔다: ${collector.lastFailure}")
        }
        assertEquals("IllegalStateException", collector.lastFailure)
    }
}
