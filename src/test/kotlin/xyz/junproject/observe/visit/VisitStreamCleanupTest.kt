package xyz.junproject.observe.visit

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.SEARCH_WORDS
import java.time.Duration
import java.time.Instant

/** 스트림 지우기의 가짜 — 부른 차례를 [events] 에 남기고, [failing] 이면 실패한다(예외 글에 주소를 싣는다 — 로그에 새지 않는지 보려고) */
class FakeVisitCleaner(private val events: MutableList<String> = mutableListOf()) : VisitStreamCleaner {
    val deleted = linkedSetOf<String>()
    val trimCutoffs = mutableListOf<Instant>()
    var failing = false
    var trimFailing = false

    override fun delete(ids: List<String>): Long {
        if (failing) { events += "delete-failed:${ids.joinToString(",")}"; throw IllegalStateException("READONLY You can't write against a read only replica $ADDRESS_V4") }
        events += "delete:${ids.joinToString(",")}"
        return ids.count { deleted.add(it) }.toLong()
    }

    override fun trimOlderThan(cutoff: Instant): Long {
        if (trimFailing) throw IllegalStateException("LOADING Redis is loading the dataset in memory $ADDRESS_V4")
        trimCutoffs += cutoff
        return 3
    }
}

/**
 * V2b — 방문 항목은 **저장 → 확인 → 지움** 의 차례로 스트림에서 지운다(주소가 든 원문이 Redis 에 90일보다 오래 남지 않게).
 * 저장에 실패한 항목은 지우지 않는다 · 꼴이 틀려 버린 항목도 확인 뒤에 지운다(주소가 든 원문이다) · 지우기의 실패는 바퀴의 실패가 아니고
 * 세어 드러내며 다음 바퀴에 다시 지운다(다시 저장하지 않는다) · 하루 한 번의 정리는 DB 삭제와 같은 기준 시각(now − 90일)으로 스트림을 잘라 낸다 ·
 * 로그 · 헬스에는 수와 종류 이름만.
 */
class VisitStreamCleanupTest {
    private val events = mutableListOf<String>()
    private val stream = FakeMetricStream(events)
    private val sink = FakeVisitSink(events)
    private val cleaner = FakeVisitCleaner(events)
    private val now = Instant.parse("2026-11-28T03:40:00Z")
    private fun collector(batchSize: Int = 10, visitSink: VisitSink = sink) =
        VisitCollector(stream, visitSink, batchSize = batchSize, block = Duration.ZERO, clock = { now }, cleaner = cleaner)
    private val logs = CapturedLogs()

    @AfterEach fun detach() = logs.close()

    private fun good(millis: Long) = VisitRecords.record("$millis-0", VisitRecords.fields(path = "/wiki/kubernetes/pod?q=$SEARCH_WORDS"))
    private fun badAddress(millis: Long) = VisitRecords.record("$millis-0", VisitRecords.fields(ip = "$ADDRESS_V4:443"))

    @Test
    fun `저장한 항목은 저장 뒤 확인하고 그 뒤에 스트림에서 지운다`() {
        stream.add(good(1_000), good(2_000))
        val collector = collector()
        assertEquals(2, collector.runOnce())
        assertEquals(listOf("insert:1000-0", "insert:2000-0", "ack:1000-0", "ack:2000-0", "delete:1000-0,2000-0"), events)
        assertEquals(2L, collector.streamDeleted)
        assertEquals(0L, collector.streamDeleteFailed)
        assertEquals(0, collector.streamDeletePending)
    }

    @Test
    fun `꼴이 틀려 버린 항목도 확인한 뒤에 지운다 — 주소가 든 원문이다`() {
        stream.add(badAddress(1_000), good(2_000))
        collector().runOnce()
        assertTrue(events.indexOf("ack:1000-0") in 0 until events.indexOfFirst { it.startsWith("delete:") && "1000-0" in it }, "확인하기 전에 지웠다: $events")
        assertEquals(setOf("1000-0", "2000-0"), cleaner.deleted)
    }

    @Test
    fun `저장에 실패한 항목은 확인도 지우기도 하지 않는다`() {
        sink.failure = IllegalStateException("connection refused")
        stream.add(good(1_000), good(2_000))
        val collector = collector()
        assertEquals(0, collector.runOnce())
        assertTrue(events.none { it.startsWith("delete") }, "저장에 실패한 항목을 지웠다: $events")
        assertEquals(setOf("1000-0", "2000-0"), stream.pending.keys)
        sink.failure = null
        collector.runOnce()
        assertEquals(setOf("1000-0", "2000-0"), cleaner.deleted, "다시 받아 저장한 뒤에는 지운다")
    }

    @Test
    fun `묶음 둘 중 뒤의 묶음만 저장에 실패하면 앞의 묶음만 지운다`() {
        var calls = 0
        val secondFails = object : VisitSink by sink {
            override fun insertAll(visits: List<Visit>): VisitInsertion {
                calls++
                if (calls == 2) throw IllegalStateException("deadlock detected")
                return sink.insertAll(visits)
            }
        }
        stream.add(good(1_000), good(2_000), good(3_000), good(4_000))
        val collector = collector(batchSize = 2, visitSink = secondFails)
        collector.runOnce()                     // 앞의 묶음(1000 · 2000)
        collector.runOnce()                     // 뒤의 묶음(3000 · 4000) — 저장 실패
        assertEquals(setOf("1000-0", "2000-0"), cleaner.deleted)
        assertEquals(setOf("3000-0", "4000-0"), stream.pending.keys, "실패한 묶음이 남지 않았다")
    }

    @Test
    fun `지우기가 실패해도 바퀴는 실패가 아니고 세어 드러내며 다음 바퀴에 다시 지우고 다시 저장하지 않는다`() {
        cleaner.failing = true
        stream.add(good(1_000), good(2_000))
        val collector = collector()
        assertEquals(2, collector.runOnce())
        assertNull(collector.lastFailure, "지우기의 실패로 수집이 실패했다")
        assertEquals(2L, collector.storedTotal)
        assertEquals(2L, collector.streamDeleteFailed)
        assertEquals(2, collector.streamDeletePending)
        assertTrue(stream.pending.isEmpty(), "지우지 못했다고 확인을 되돌렸다")
        cleaner.failing = false
        collector.runOnce()
        assertEquals(setOf("1000-0", "2000-0"), cleaner.deleted, "다음 바퀴에 다시 지우지 않았다")
        assertEquals(0, collector.streamDeletePending)
        assertEquals(2L, collector.storedTotal, "다시 저장했다")
        assertEquals(2, events.count { it.startsWith("insert:") }, "다시 저장했다: $events")
        val text = logs.text()
        assertTrue(text.contains("visit stream delete failed for 2 entries"), "지우기 실패가 로그에 수로 드러나지 않았다: $text")
        assertFalse(text.contains(ADDRESS_V4), "지우기 실패의 로그에 예외 글(주소)이 실렸다")
        assertFalse(text.contains(SEARCH_WORDS))
    }

    @Test
    fun `지우개가 없으면 기존 동작 그대로 지우지 않는다`() {
        stream.add(good(1_000))
        VisitCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now }).runOnce()
        assertEquals(listOf("insert:1000-0", "ack:1000-0"), events)
    }

    @Test
    fun `헬스의 visits 칸에 지운 수 · 실패 수 · 기다리는 수와 마지막 정리가 실리고 주소는 없다`() {
        cleaner.failing = true
        stream.add(good(1_000), good(2_000))
        val collector = collector()
        collector.runOnce()
        cleaner.failing = false
        stream.add(good(3_000))
        collector.runOnce()
        val trim = VisitStreamTrimResult(now, 7, null)
        val report = VisitHealth(collector, { 0L }, { 0L }, { null }, { null }, { null }, startedAt = now.minusSeconds(1), lastStreamTrim = { trim }).report(now)
        assertEquals(3L, report["stream_deleted"])
        assertEquals(2L, report["stream_delete_failed"])
        assertEquals(0L, report["stream_delete_pending"])
        assertEquals(now.toString(), report["last_stream_trim_at"])
        assertEquals(7L, report["last_stream_trimmed"])
        assertEquals("", report["last_stream_trim_failure"])
        assertEquals("ok", report["status"], "지우기의 실패가 지나간 뒤 상태가 ok 가 아니다")
        val failed = VisitHealth(collector, { 0L }, { 0L }, { null }, { null }, { null }, startedAt = now.minusSeconds(1),
            lastStreamTrim = { VisitStreamTrimResult(now, null, "IllegalStateException") }).report(now)
        assertEquals(-1L, failed["last_stream_trimmed"])
        assertEquals("IllegalStateException", failed["last_stream_trim_failure"])
        val untouched = VisitHealth(collector, { 0L }, { 0L }, { null }, { null }, { null }, startedAt = now.minusSeconds(1)).report(now)
        assertEquals("", untouched["last_stream_trim_at"])
        assertEquals(-1L, untouched["last_stream_trimmed"])
        assertFalse(report.toString().contains(ADDRESS_V4))
    }

    private class CountingRetention(var failing: Boolean = false) : VisitRetention {
        val cutoffs = mutableListOf<Instant>()
        override fun deleteOlderThan(cutoff: Instant, limit: Int): Int { if (failing) throw IllegalStateException("db down"); cutoffs += cutoff; return 0 }
    }

    @Test
    fun `하루 한 번의 정리는 DB 삭제와 같은 기준 시각 now − 90일로 스트림을 잘라 내고 결과를 남긴다`() {
        val retention = CountingRetention()
        val schedule = VisitSchedule(collector(), retention, cleaner = cleaner)
        schedule.deleteOldVisits(now)
        assertEquals(listOf(now.minus(Duration.ofDays(90))), cleaner.trimCutoffs)
        assertEquals(retention.cutoffs.single(), cleaner.trimCutoffs.single(), "스트림과 DB 의 기준 시각이 다르다")
        assertEquals(VisitStreamTrimResult(now, 3, null), schedule.lastStreamTrim)
        assertTrue(logs.lines().any { it.contains("trimmed 3 visit stream entries") }, "${logs.lines()}")
    }

    @Test
    fun `정리가 실패하면 종류 이름만 남기고 DB 삭제는 그대로 돌며 DB 삭제가 실패해도 정리는 돈다`() {
        cleaner.trimFailing = true
        val retention = CountingRetention()
        val schedule = VisitSchedule(collector(), retention, cleaner = cleaner)
        schedule.deleteOldVisits(now)
        assertEquals(VisitStreamTrimResult(now, null, "IllegalStateException"), schedule.lastStreamTrim)
        assertEquals(1, retention.cutoffs.size, "정리의 실패로 DB 삭제가 돌지 않았다")
        assertNull(schedule.lastRetention?.failure)
        assertFalse(logs.text().contains(ADDRESS_V4), "정리 실패의 로그에 예외 글이 실렸다")
        assertTrue(logs.errorLines().any { it.contains("visit stream trim failed: IllegalStateException") }, "${logs.errorLines()}")

        cleaner.trimFailing = false
        val brokenDatabase = VisitSchedule(collector(), CountingRetention(failing = true), cleaner = cleaner)
        brokenDatabase.deleteOldVisits(now)
        assertEquals(VisitStreamTrimResult(now, 3, null), brokenDatabase.lastStreamTrim, "DB 삭제가 실패하자 정리가 돌지 않았다")
        assertNotNull(brokenDatabase.lastRetention?.failure)
    }
}
