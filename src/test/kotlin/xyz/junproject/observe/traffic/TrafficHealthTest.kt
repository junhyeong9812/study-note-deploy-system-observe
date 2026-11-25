package xyz.junproject.observe.traffic

import org.junit.jupiter.api.Assertions.*
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogSink
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.metric.FakeMetricStream
import java.time.Duration
import java.time.Instant

/**
 * 헬스의 `traffic` 칸 — 받은 수 · 버린 수와 이유 · 순번으로 버린 수 · 칸마다 버린 수 · `at` 의 앞섬 · 오래됨 · 서버마다의 틈 · 읽기 진도 · 수신 지연 ·
 * 마지막 저장 실패. 통신 쪽이 어떻게 실패해도 기존 헬스의 판정(`status`)과 HTTP 상태는 그대로다.
 */
class TrafficHealthTest {
    private var now = Instant.parse("2026-11-25T04:00:00Z")
    private val stream = FakeMetricStream()
    private val sink = FakeTrafficSink()
    private val collector = TrafficCollector(stream, sink, batchSize = 10, block = Duration.ZERO, clock = { now })

    private fun health(
        unacknowledged: () -> Long = { 0L },
        latestStoredAt: () -> Instant? = { null },
        progress: () -> List<TrafficServerProgress> = { emptyList() },
    ) = TrafficHealth(collector, unacknowledged, { 0L }, latestStoredAt, progress, { null }, { null }, { null }, startedAt = now.minusSeconds(1))

    @Test
    fun `헬스의 traffic 칸은 받은 수 · 버린 수 · 순번으로 버린 수 · 칸마다 버린 수 · at 의 수 · 마지막 저장 실패를 낸다`() {
        sink.outcome = { TrafficStoreOutcome(false, segmentsAhead = 1, requestsTooOld = 2) }
        stream.add(
            TrafficRecords.record("1000-0", 1, now.plus(Duration.ofMinutes(3))),
            TrafficRecords.record("2000-0", 1, now),
            TrafficRecords.record("3000-0", 2, now).let { it.copy(fields = it.fields + ("v" to "9")) },
        )
        collector.runOnce()
        sink.failing = { true }
        stream.add(TrafficRecords.record("4000-0", 3, now))
        collector.runOnce()
        val report = health().report(now)
        assertEquals(1L, report["stored_since_start"])
        assertEquals(1L, report["duplicates"])
        assertEquals(1L, report["dropped_total"])
        @Suppress("UNCHECKED_CAST")
        val dropped = report["dropped"] as Map<String, Long>
        assertEquals(TrafficDropReason.entries.map { it.wireName }.toSet(), dropped.keys)
        assertEquals(1L, dropped["unsupported_version"])
        assertEquals(mapOf("segment_ahead" to 1L, "segment_too_old" to 0L, "request_ahead" to 0L, "request_too_old" to 2L), report["cells_dropped"])
        assertEquals(1L, report["at_ahead"])
        assertEquals(0L, report["at_too_old"])
        assertEquals(0L, report["requests_rejected"])
        assertEquals("IllegalStateException", report["last_store_failure"])
        assertEquals(now.toString(), report["last_store_failure_at"])
        assertEquals("failing", report["status"])
        assertFalse(report.toString().contains("DB 에 닿지 못함"), "헬스에 예외 문구가 실렸다")
    }

    @Test
    fun `서버마다 읽기 진도 · 수신 지연 · 순번의 틈 · 마지막 수신을 싣고 읽지 못하면 빈 목록이다`() {
        val progress = listOf(
            TrafficServerProgress("ci-agent-158", now.minusSeconds(40), now.minusSeconds(10), 2, now.minusSeconds(10), firstSeqSeen = 1),
            TrafficServerProgress("ci-agent-164", null, null, 0, now.minus(Duration.ofMinutes(9)), firstSeqSeen = 5),
        )
        val report = health(progress = { progress }).report(now)
        assertEquals(
            listOf(
                mapOf("server" to "ci-agent-158", "read_progress_at" to now.minusSeconds(40).toString(), "receive_delayed" to false, "sequence_gaps" to 2L,
                    "last_received_at" to now.minusSeconds(10).toString(), "first_seq_seen" to 1L),
                mapOf("server" to "ci-agent-164", "read_progress_at" to "", "receive_delayed" to true, "sequence_gaps" to 0L,
                    "last_received_at" to now.minus(Duration.ofMinutes(9)).toString(), "first_seq_seen" to 5L),
            ),
            report["servers"],
        )
        assertEquals(emptyList<Any>(), health(progress = { throw IllegalStateException("table locked") }).report(now)["servers"])
    }

    @Test
    fun `Redis 나 DB 에 닿지 못하면 traffic 의 status 는 unreachable 이고 사유는 종류 이름뿐이다`() {
        val report = health(unacknowledged = { throw IllegalStateException("redis down MARKER") }).report(now)
        assertEquals("unreachable", report["status"])
        assertEquals("redis IllegalStateException", report["failure"])
    }

    private class QuietLogSink : LogSink {
        override fun createSchema() {}
        override fun insertAll(entries: List<LogEntry>): Int = entries.size
    }

    @Test
    fun `지표 칸이 실패하면 기존 로그 그대로 종류와 문구 200자까지 · 통신 칸이 실패하면 종류만 남긴다`() {
        val logCollector = LogCollector(FakeLogStream(), QuietLogSink(), block = Duration.ZERO, clock = { now })
        logCollector.runOnce()
        val metric = Mockito.mock(MetricHealth::class.java)
        Mockito.doThrow(IllegalStateException("metric detail " + "m".repeat(300))).`when`(metric).report(now)
        val traffic = Mockito.mock(TrafficHealth::class.java)
        Mockito.doThrow(IllegalStateException("traffic detail MARKER-path")).`when`(traffic).report(now)
        val logs = ListAppender<ILoggingEvent>().also { it.start() }
        val logger = LoggerFactory.getLogger("observe") as Logger
        logger.addAppender(logs)
        val report = try {
            CollectorHealth(logCollector, { 0L }, { 0L }, { null }, { null }, startedAt = now.minusSeconds(1), metrics = metric, traffic = traffic).report(now)
        } finally {
            logger.detachAppender(logs)
        }
        val lines = logs.list.map { it.formattedMessage }
        val metricText = ("metric detail " + "m".repeat(300)).take(200)
        assertTrue(lines.contains("observe:observe:metric health failed: IllegalStateException: $metricText"), "지표 헬스 실패 로그가 기존과 다르다: $lines")
        assertTrue(lines.contains("observe:observe:traffic health failed: IllegalStateException"), "통신 헬스 실패 로그가 종류만이 아니다: $lines")
        assertFalse(lines.any { it.contains("MARKER") }, "통신 쪽 예외 문구가 로그에 실렸다: $lines")
        assertTrue(report.healthy, "칸의 실패가 판정을 바꿨다")
        assertEquals(mapOf("status" to "failing", "failure" to "health IllegalStateException"), report.body["metrics"])
        assertEquals(mapOf("status" to "failing", "failure" to "health IllegalStateException"), report.body["traffic"])
    }

    @Test
    fun `통신 칸이 실패하거나 unreachable 이어도 로그 수집의 status 와 HTTP 판정은 그대로다`() {
        val logCollector = LogCollector(FakeLogStream(), QuietLogSink(), block = Duration.ZERO, clock = { now })
        logCollector.runOnce()
        val broken = TrafficHealth(collector, { throw IllegalStateException("redis down") }, { 0L }, { throw IllegalStateException("db down") },
            { throw IllegalStateException("x") }, { null }, { null }, { null })
        val withTraffic = CollectorHealth(logCollector, { 0L }, { 0L }, { null }, { null }, startedAt = now.minusSeconds(1), traffic = broken).report(now)
        val without = CollectorHealth(logCollector, { 0L }, { 0L }, { null }, { null }, startedAt = now.minusSeconds(1)).report(now)
        assertTrue(without.healthy)
        assertEquals(without.healthy, withTraffic.healthy)
        assertEquals(without.body["status"], withTraffic.body["status"])
        @Suppress("UNCHECKED_CAST")
        assertEquals("unreachable", (withTraffic.body["traffic"] as Map<String, Any?>)["status"])
        assertNull(without.body["traffic"])
        assertEquals(without.body.keys + "traffic", withTraffic.body.keys, "기존 칸이 바뀌었다")
    }
}
