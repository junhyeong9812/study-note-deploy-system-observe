package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.BadSqlGrammarException
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import java.sql.SQLException
import java.time.Duration
import java.time.Instant

/**
 * 명세 §10.3 헬스 — 지표 칸(마지막 저장 시각 · 버린 항목 수(이유별) · 밀린 건수 · 5분 평균의 마지막 성공 시각과 실패 사유 · 삭제 결과)과
 * §2.3 서버별 마지막 수신 시각. **지표 쪽이 실패해도 로그 수집의 `status` 와 HTTP 상태는 그대로.**
 */
class MetricHealthTest {
    private val startedAt = Instant.parse("2026-11-24T03:00:00Z")
    private var now = startedAt
    private val stream = FakeMetricStream()
    private val sink = FakeMetricSink()
    private val collector = MetricCollector(stream, sink, block = Duration.ZERO, clock = { now })
    private var databaseReachable = true
    private var tableMissing = false
    private var latestInDatabase: Instant? = null
    private var receivedByServer: () -> Map<String, Instant> = { emptyMap() }
    private var lag: () -> Long? = { 0L }
    private var rollup: MetricRollupResult? = null
    private var rollupSuccessAt: Instant? = null
    private var retention: MetricRetentionResult? = null

    private val metricHealth = MetricHealth(
        collector,
        unacknowledged = { stream.unacknowledgedCount() },
        lag = { lag() },
        latestStoredAt = {
            if (!databaseReachable) throw IllegalStateException("DB 에 닿지 못함 jdbc:postgresql://postgres:5432")
            if (tableMissing) throw BadSqlGrammarException("latest", "SELECT …", SQLException("relation does not exist", "42P01"))
            latestInDatabase
        },
        lastReceivedByServer = { receivedByServer() },
        lastRollup = { rollup },
        lastRollupSuccessAt = { rollupSuccessAt },
        lastRetention = { retention },
        startedAt = startedAt,
    )

    private fun report() = metricHealth.report(now)

    @Test
    fun `한 바퀴를 돌았으면 ok 이고 칸이 모두 있다`() {
        stream.add(MetricRecords.resource("1000-0", now.minusSeconds(1)))
        collector.runOnce()
        latestInDatabase = now
        receivedByServer = { mapOf("ci-agent-158" to now, "ci-agent-9" to now.minusSeconds(31)) }
        val body = report()
        assertEquals(setOf(
            "status", "last_loop_at", "last_stored_at", "stored_since_start", "unacknowledged", "lag", "failure",
            "dropped", "dropped_total", "skipped_other_kind", "last_received_at",
            "last_rollup_at", "last_rollup_failure",
            "last_retention_at", "last_retention_raw_deleted", "last_retention_rollup_deleted", "last_retention_failure",
        ), body.keys)
        assertEquals("ok", body["status"])
        assertEquals(now.toString(), body["last_loop_at"])
        assertEquals(now.toString(), body["last_stored_at"])
        assertEquals(1L, (body["stored_since_start"] as Number).toLong())
        assertEquals(0L, (body["unacknowledged"] as Number).toLong())
        assertEquals(0L, (body["lag"] as Number).toLong())
        assertEquals("", body["failure"])
        assertEquals(MetricDropReason.entries.associate { it.wireName to 0L }, (body["dropped"] as Map<*, *>).mapValues { (it.value as Number).toLong() })
        assertEquals(0L, (body["dropped_total"] as Number).toLong())
        assertEquals(0L, (body["skipped_other_kind"] as Number).toLong())
        assertEquals(mapOf("ci-agent-158" to now.toString(), "ci-agent-9" to now.minusSeconds(31).toString()), body["last_received_at"])
        assertEquals("", body["last_rollup_at"])
        assertEquals("", body["last_rollup_failure"])
        assertEquals("", body["last_retention_at"])
        assertEquals(-1L, (body["last_retention_raw_deleted"] as Number).toLong())
        assertEquals(-1L, (body["last_retention_rollup_deleted"] as Number).toLong())
        assertEquals("", body["last_retention_failure"])
    }

    @Test
    fun `버린 항목은 이유 글자마다 세고 합계와 다른 종류 건너뜀이 따로 나온다`() {
        val broken = MetricRecords.resource("1000-0", now).let { it.copy(fields = it.fields + ("body" to "[]")) }
        val ahead = MetricRecords.resource("2000-0", now.plus(Duration.ofMinutes(5)))
        stream.add(broken, ahead, MetricStreamRecord("3000-0", mapOf("kind" to "trace")))
        collector.runOnce()
        val body = report()
        val dropped = (body["dropped"] as Map<*, *>).mapValues { (it.value as Number).toLong() }
        assertEquals(1L, dropped["invalid_body"])
        assertEquals(1L, dropped["clock_ahead"])
        assertEquals(2L, (body["dropped_total"] as Number).toLong())
        assertEquals(1L, (body["skipped_other_kind"] as Number).toLong())
    }

    @Test
    fun `5분 평균은 마지막 성공 시각과 마지막 바퀴의 실패 사유를 따로 낸다`() {
        val succeeded = now.minus(Duration.ofMinutes(3))
        rollup = MetricRollupResult(succeeded, 4, null)
        rollupSuccessAt = succeeded
        assertEquals(succeeded.toString() to "", report().let { it["last_rollup_at"] to it["last_rollup_failure"] })
        rollup = MetricRollupResult(now, null, "QueryTimeoutException")
        assertEquals(succeeded.toString() to "QueryTimeoutException", report().let { it["last_rollup_at"] to it["last_rollup_failure"] })
    }

    @Test
    fun `삭제의 시각·건수·실패 사유가 나온다`() {
        retention = MetricRetentionResult(Instant.parse("2026-11-24T03:40:00Z"), MetricDeletion(5_760, 288), null)
        report().let {
            assertEquals("2026-11-24T03:40:00Z", it["last_retention_at"])
            assertEquals(5_760L, (it["last_retention_raw_deleted"] as Number).toLong())
            assertEquals(288L, (it["last_retention_rollup_deleted"] as Number).toLong())
            assertEquals("", it["last_retention_failure"])
        }
        retention = MetricRetentionResult(Instant.parse("2026-11-25T03:40:00Z"), null, "CannotGetJdbcConnectionException")
        report().let {
            assertEquals("2026-11-25T03:40:00Z", it["last_retention_at"])
            assertEquals(-1L, (it["last_retention_raw_deleted"] as Number).toLong())
            assertEquals(-1L, (it["last_retention_rollup_deleted"] as Number).toLong())
            assertEquals("CannotGetJdbcConnectionException", it["last_retention_failure"])
        }
    }

    @Test
    fun `지표 수집 바퀴가 실패하면 failing, DB 에 닿지 못하면 unreachable 이고 사유는 종류 이름만`() {
        stream.add(MetricRecords.resource("1000-0", now))
        sink.failing = true
        collector.runOnce()
        assertEquals("failing" to "IllegalStateException", report().let { it["status"] to it["failure"] })
        databaseReachable = false
        assertEquals("unreachable" to "database IllegalStateException", report().let { it["status"] to it["failure"] })
        assertEquals("", report()["last_stored_at"])
    }

    @Test
    fun `Redis 에 닿지 못하면 unreachable, 밀린 건수나 서버별 수신 시각을 읽지 못하면 -1 과 빈 객체`() {
        collector.runOnce()
        lag = { throw IllegalStateException("ERR") }
        receivedByServer = { throw IllegalStateException("timeout") }
        val body = report()
        assertEquals("ok", body["status"], "밀린 건수·수신 시각을 못 읽은 것만으로 판정이 바뀌었다")
        assertEquals(-1L, (body["lag"] as Number).toLong())
        assertEquals(emptyMap<String, String>(), body["last_received_at"])
        stream.reachable = false
        assertEquals("unreachable" to "redis IllegalStateException", report().let { it["status"] to it["failure"] })
        assertEquals(-1L, (report()["unacknowledged"] as Number).toLong())
    }

    @Test
    fun `표가 아직 없다는 답은 닿은 것이라 unreachable 이 아니다`() {
        tableMissing = true
        stream.ensureGroup()
        assertEquals("starting", report()["status"])
    }

    @Test
    fun `첫 바퀴 전에는 starting 이고 1분이 지나도 바퀴가 없으면 stalled`() {
        stream.ensureGroup()
        assertEquals("starting", report()["status"])
        now = startedAt.plusSeconds(61)
        assertEquals("stalled", report()["status"])
        collector.runOnce()
        assertEquals("ok", report()["status"])
        now = now.plusSeconds(120)
        assertEquals("stalled", report()["status"])
    }

    // ── /health 와 합칠 때 ───────────────────────────────

    private val logStream = FakeLogStream()
    private val logCollector = LogCollector(logStream, FakeLogSink(), block = Duration.ZERO, clock = { now })

    private fun collectorHealth(metrics: MetricHealth?) =
        CollectorHealth(logCollector, { logStream.unacknowledgedCount() }, { 0L }, { null }, { null }, startedAt = startedAt, metrics = metrics)

    @Test
    fun `지표가 실패해도 로그의 status 와 HTTP 판정은 그대로이고 지표 칸은 metrics 객체에 따로 나온다`() {
        logCollector.runOnce()
        stream.add(MetricRecords.resource("1000-0", now))
        sink.failing = true
        collector.runOnce()
        databaseReachable = false
        val report = collectorHealth(metricHealth).report(now)
        assertTrue(report.healthy, "지표 실패가 로그 수집의 판정을 바꿨다")
        assertEquals("ok", report.body["status"])
        assertEquals("", report.body["failure"], "지표 실패가 로그의 failure 칸에 섞였다")
        @Suppress("UNCHECKED_CAST")
        val metrics = report.body["metrics"] as Map<String, Any?>
        assertEquals("unreachable", metrics["status"])
    }

    @Test
    fun `지표 칸을 더해도 기존 칸은 이름도 값도 그대로다`() {
        logCollector.runOnce()
        collector.runOnce()
        val without = collectorHealth(null).report(now)
        val with = collectorHealth(metricHealth).report(now)
        assertEquals(without.healthy, with.healthy)
        assertEquals(without.body, with.body - "metrics")
        assertEquals(without.body.keys + "metrics", with.body.keys)
    }

    @Test
    fun `지표를 붙이지 않으면 metrics 칸이 없다 — 기존 헬스 그대로`() {
        logCollector.runOnce()
        val body = collectorHealth(null).report(now).body
        assertFalse(body.containsKey("metrics"))
        assertEquals("ok", body["status"])
    }
}
