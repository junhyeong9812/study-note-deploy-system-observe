package xyz.junproject.observe.visit

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import java.time.Duration
import java.time.Instant

/**
 * V2b 리뷰 루프 1 — 다시 지울 ID 의 목록에 상한: 상한(`deletePendingLimit`, 기본 1만)을 넘으면 **가장 오래된 것부터** 잊고 그 수를 센다
 * (`streamDeleteForgotten` · 헬스 `stream_delete_forgotten` · 넘친 바퀴에 로그 한 줄 — 수와 안전망만). 다음 바퀴의 다시 지우기는
 * `deleteRetryBatchSize`(기본 1천)씩 오래된 것부터 — 한 묶음이 실패하면 그 바퀴의 나머지 묶음은 하지 않는다 · 성공한 묶음만 목록에서 빠진다.
 */
class VisitStreamDeleteLimitTest {
    private val events = mutableListOf<String>()
    private val stream = FakeMetricStream(events)
    private val sink = FakeVisitSink(events)
    private val now = Instant.parse("2026-11-28T03:40:00Z")
    private val logs = CapturedLogs()

    @AfterEach fun detach() = logs.close()

    /** 지우기의 가짜 — [failAfter] 번째 부름부터 실패한다(null 이면 [failing] 을 따른다). 부를 때마다 받은 ID 를 남긴다 */
    private class ScriptedCleaner : VisitStreamCleaner {
        val calls = mutableListOf<List<String>>()
        var failing = false
        var failFromCall: Int? = null
        override fun delete(ids: List<String>): Long {
            calls += ids
            val failNow = failFromCall?.let { calls.size >= it } ?: failing
            if (failNow) throw IllegalStateException("READONLY $ADDRESS_V4")
            return ids.size.toLong()
        }
        override fun trimOlderThan(cutoff: Instant): Long = 0
    }

    private val cleaner = ScriptedCleaner()
    private fun collector(limit: Int = 10_000, retryBatch: Int = 1_000) = VisitCollector(stream, sink, batchSize = 50, block = Duration.ZERO, clock = { now },
        cleaner = cleaner, deletePendingLimit = limit, deleteRetryBatchSize = retryBatch)

    private fun good(millis: Long) = VisitRecords.record("$millis-0")
    private fun forgetLines() = logs.lines().filter { it.contains("visit stream delete pending list over") }

    @Test
    fun `상한을 넘으면 가장 오래된 것부터 잊고 잊은 수를 세며 넘친 바퀴마다 로그 한 줄을 남긴다`() {
        cleaner.failing = true
        val collector = collector(limit = 3)
        stream.add(*Array(5) { good(1_000L * (it + 1)) })
        collector.runOnce()
        assertEquals(3, collector.streamDeletePending)
        assertEquals(2L, collector.streamDeleteForgotten)
        stream.add(good(6_000), good(7_000))
        collector.runOnce()                                                     // 처음에 다시 지우기(실패) → 새 둘도 실패 → 넘침
        assertEquals(3, collector.streamDeletePending)
        assertEquals(4L, collector.streamDeleteForgotten)
        cleaner.failing = false
        collector.runOnce()
        assertEquals(listOf("5000-0", "6000-0", "7000-0"), cleaner.calls.last(), "새 것이 아니라 오래된 것부터 잊어야 한다 — 남은 것은 가장 새 셋")
        assertEquals(0, collector.streamDeletePending)
        assertEquals(listOf("observe:observe:visit stream delete pending list over 3 — forgot the 2 oldest entries, the daily trim removes them",
            "observe:observe:visit stream delete pending list over 3 — forgot the 2 oldest entries, the daily trim removes them"), forgetLines())
        assertFalse(logs.text().contains(ADDRESS_V4))
        assertFalse(forgetLines().any { it.contains("000-0") }, "잊은 항목의 ID 가 로그에 실렸다")
    }

    @Test
    fun `한 바퀴에 여러 번 넘쳐도 로그는 그 바퀴에 한 줄이고 수는 합이다`() {
        cleaner.failing = true
        val collector = collector(limit = 1)
        // 버린 항목 둘의 지우기(실패 — 넘침 1) · 저장한 항목 둘의 지우기(실패 — 넘침 2): 한 바퀴에 두 번 넘친다
        stream.add(VisitRecords.record("1000-0", VisitRecords.fields(ip = "$ADDRESS_V4:443")), VisitRecords.record("2000-0", VisitRecords.fields(ip = "$ADDRESS_V4:443")),
            good(3_000), good(4_000))
        collector.runOnce()
        assertEquals(1, collector.streamDeletePending)
        assertEquals(3L, collector.streamDeleteForgotten)
        assertEquals(listOf("observe:observe:visit stream delete pending list over 1 — forgot the 3 oldest entries, the daily trim removes them"), forgetLines())
    }

    @Test
    fun `상한 안에서는 잊지 않고 로그도 없다`() {
        cleaner.failing = true
        val collector = collector(limit = 5)
        stream.add(*Array(5) { good(1_000L * (it + 1)) })
        collector.runOnce()
        assertEquals(5, collector.streamDeletePending)
        assertEquals(0L, collector.streamDeleteForgotten)
        assertEquals(emptyList<String>(), forgetLines())
    }

    @Test
    fun `다음 바퀴의 다시 지우기는 정한 수씩 오래된 것부터 나눠 하고 수가 맞는다`() {
        cleaner.failing = true
        val collector = collector(retryBatch = 2)
        stream.add(*Array(5) { good(1_000L * (it + 1)) })
        collector.runOnce()
        assertEquals(5L, collector.streamDeleteFailed)
        cleaner.failing = false
        cleaner.calls.clear()
        collector.runOnce()
        assertEquals(listOf(listOf("1000-0", "2000-0"), listOf("3000-0", "4000-0"), listOf("5000-0")), cleaner.calls)
        assertEquals(0, collector.streamDeletePending)
        assertEquals(5L, collector.streamDeleted)
        assertEquals(5L, collector.streamDeleteFailed)
    }

    @Test
    fun `다시 지우기의 한 묶음이 실패하면 그 바퀴의 나머지 묶음은 하지 않고 성공한 묶음만 목록에서 빠진다`() {
        cleaner.failing = true
        val collector = collector(retryBatch = 2)
        stream.add(*Array(5) { good(1_000L * (it + 1)) })
        collector.runOnce()
        cleaner.failing = false
        cleaner.calls.clear()
        cleaner.failFromCall = 2                                                // 둘째 묶음부터 실패
        collector.runOnce()
        assertEquals(listOf(listOf("1000-0", "2000-0"), listOf("3000-0", "4000-0")), cleaner.calls, "실패한 뒤에도 그 바퀴에 묶음을 더 했다")
        assertEquals(3, collector.streamDeletePending)
        assertEquals(2L, collector.streamDeleted)
        assertEquals(5L + 2L, collector.streamDeleteFailed)
        cleaner.failFromCall = null
        cleaner.calls.clear()
        collector.runOnce()
        assertEquals(listOf(listOf("3000-0", "4000-0"), listOf("5000-0")), cleaner.calls)
        assertEquals(0, collector.streamDeletePending)
        assertEquals(5L, collector.streamDeleted)
        assertNull(collector.lastFailure)
    }

    @Test
    fun `기본값은 상한 1만 · 다시 지우기 1천씩이다`() {
        cleaner.failing = true
        val collector = VisitCollector(stream, sink, batchSize = 10_001, block = Duration.ZERO, clock = { now }, cleaner = cleaner)
        stream.add(*Array(10_001) { good(1_000L + it) })
        collector.runOnce()
        assertEquals(10_000, collector.streamDeletePending)
        assertEquals(1L, collector.streamDeleteForgotten)
        cleaner.failing = false
        cleaner.calls.clear()
        collector.runOnce()
        assertEquals(List(10) { 1_000 }, cleaner.calls.map { it.size })
        assertEquals("1001-0", cleaner.calls.first().first(), "가장 오래된 하나(1000-0)를 잊고 그다음부터 남아야 한다")
    }

    @Test
    fun `헬스의 visits 칸에 잊은 수가 실린다`() {
        cleaner.failing = true
        val collector = collector(limit = 1)
        stream.add(good(1_000), good(2_000), good(3_000))
        collector.runOnce()
        val report = VisitHealth(collector, { 0L }, { 0L }, { null }, { null }, { null }, startedAt = now.minusSeconds(1)).report(now)
        assertEquals(2L, report["stream_delete_forgotten"])
        assertEquals(1L, report["stream_delete_pending"])
    }
}
