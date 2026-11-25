package xyz.junproject.observe.visit

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.BadSqlGrammarException
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch

/**
 * 헬스의 `visits` 칸 — 지표 칸과 같은 낱말(status · last_loop_at · last_stored_at · stored_since_start · unacknowledged · lag · failure)
 * + 버린 수(이유별) · 잘라 낸 수 · 90일 삭제의 결과. 실패 사유는 예외 종류 이름만(헬스는 인증이 없다 — 주소는 물론 문구도 없다).
 * **방문 쪽이 어떻게 실패해도 로그 수집의 `status` · HTTP 상태와 지표 칸은 그대로다.**
 */
class VisitHealthTest {
    private val startedAt = Instant.parse("2026-11-25T03:00:00Z")
    private var now = startedAt
    private val stream = FakeMetricStream()
    private val sink = FakeVisitSink()
    private val collector = VisitCollector(stream, sink, block = Duration.ZERO, clock = { now })
    private var databaseReachable = true
    private var tableMissing = false
    private var latestInDatabase: Instant? = null
    private var retention: RetentionResult? = null

    private fun visitHealth(latestStoredAt: () -> Instant? = {
        if (!databaseReachable) throw IllegalStateException("DB 에 닿지 못함 jdbc:postgresql://postgres:5432 $ADDRESS_V4")
        if (tableMissing) throw BadSqlGrammarException("latest", "SELECT …", SQLException("relation \"visit_event\" does not exist", "42P01"))
        latestInDatabase
    }, readBudget: Duration = Duration.ofSeconds(2)) = VisitHealth(
        collector,
        unacknowledged = { stream.unacknowledgedCount() },
        lag = { 0L },
        latestStoredAt = latestStoredAt,
        lastRetention = { retention },
        startedAt = startedAt,
        readBudget = readBudget,
    )

    @Test
    fun `한 바퀴를 돌았으면 ok 이고 칸이 모두 있다`() {
        stream.add(VisitRecords.record("1000-0"), VisitRecords.record("2000-0", VisitRecords.fields(device = "tablet")))
        collector.runOnce()
        latestInDatabase = now
        val body = visitHealth().report(now)
        assertEquals(setOf(
            "status", "last_loop_at", "last_stored_at", "stored_since_start", "unacknowledged", "lag", "failure",
            "dropped", "dropped_total", "query_stripped", "last_retention_at", "last_retention_deleted", "last_retention_failure",
        ), body.keys)
        assertEquals("ok", body["status"])
        assertEquals(now.toString(), body["last_stored_at"])
        assertEquals(1L, body["stored_since_start"])
        assertEquals(0L, body["unacknowledged"])
        assertEquals("", body["failure"])
        assertEquals(VisitDropReason.entries.associate { it.wireName to if (it == VisitDropReason.INVALID_DEVICE) 1L else 0L }, body["dropped"])
        assertEquals(1L, body["dropped_total"])
        assertEquals(0L, body["query_stripped"])
        assertEquals("", body["last_retention_at"])
        assertEquals(-1, body["last_retention_deleted"])
    }

    @Test
    fun `DB 에 닿지 못하면 unreachable 이고 사유는 종류 이름만이다`() {
        collector.runOnce()
        databaseReachable = false
        val body = visitHealth().report(now)
        assertEquals("unreachable", body["status"])
        assertEquals("database IllegalStateException", body["failure"])
        assertFalse(body.toString().contains(ADDRESS_V4), "$body")
        assertFalse(body.toString().contains("jdbc:"), "$body")
    }

    @Test
    fun `Redis 에 닿지 못하면 unreachable`() {
        collector.runOnce()
        stream.reachable = false
        val body = visitHealth().report(now)
        assertEquals("unreachable", body["status"])
        assertTrue((body["failure"] as String).startsWith("redis "), "$body")
    }

    @Test
    fun `저장이 실패한 바퀴 뒤에는 failing 이고 미확인 건수가 남은 항목 수다`() {
        stream.add(VisitRecords.record("1000-0"), VisitRecords.record("2000-0"))
        sink.failure = IllegalStateException("Batch entry 0 … ('$ADDRESS_V4') was aborted")
        collector.runOnce()
        val body = visitHealth().report(now)
        assertEquals("failing", body["status"])
        assertEquals("IllegalStateException", body["failure"])
        assertEquals(2L, body["unacknowledged"], "저장에 실패한 항목을 확인해 버렸다 — 잃는다")
        assertFalse(body.toString().contains(ADDRESS_V4), "$body")
    }

    @Test
    fun `표가 아직 없으면 닿은 것으로 보고 첫 바퀴 전이면 starting 이며 1분이 지나면 stalled`() {
        stream.ensureGroup()                                   // 그룹은 있고(Redis 에 닿는다) 표만 아직 없다
        tableMissing = true
        assertEquals("starting", visitHealth().report(now)["status"])
        assertEquals("", visitHealth().report(now)["last_stored_at"])
        assertEquals("stalled", visitHealth().report(now.plusSeconds(61))["status"])
    }

    @Test
    fun `90일 삭제의 마지막 결과를 보인다`() {
        collector.runOnce()
        retention = RetentionResult(now, 42, null)
        var body = visitHealth().report(now)
        assertEquals(now.toString(), body["last_retention_at"])
        assertEquals(42, body["last_retention_deleted"])
        assertEquals("", body["last_retention_failure"])
        retention = RetentionResult(now, null, "QueryTimeoutException")
        body = visitHealth().report(now)
        assertEquals("QueryTimeoutException", body["last_retention_failure"])
        assertEquals("ok", body["status"], "삭제 실패가 수집의 상태를 바꿨다")
    }

    @Test
    fun `마지막 저장 시각을 읽는 것이 멈춰도 읽기 상한 안에 unreachable 로 답한다`() {
        collector.runOnce()
        val never = CountDownLatch(1)
        val started = System.nanoTime()
        val body = visitHealth(latestStoredAt = { never.await(); null }, readBudget = Duration.ofMillis(300)).report(now)
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed < Duration.ofSeconds(2), "헬스가 $elapsed 기다렸다")
        assertEquals("unreachable", body["status"])
        assertEquals("database TimeoutException", body["failure"])
    }

    // ── 로그 · 지표의 헬스를 바꾸지 않는다 ─────────────────

    private val logStream = FakeLogStream()
    private val logCollector = LogCollector(logStream, FakeLogSink(), block = Duration.ZERO, clock = { now })
    private val metricStream = FakeMetricStream()
    private val metricCollector = MetricCollector(metricStream, FakeMetricSink(), block = Duration.ZERO, clock = { now })
    private val metricHealth = MetricHealth(metricCollector, { metricStream.unacknowledgedCount() }, { 0L }, { now }, { emptyMap() },
        { null }, { null }, { null }, startedAt = startedAt)

    private fun collectorHealth(visits: VisitHealth?) = CollectorHealth(logCollector, { logStream.unacknowledgedCount() }, { 0L }, { now }, { null },
        startedAt = startedAt, metrics = metricHealth, visits = visits)

    @Test
    fun `헬스 본문에 visits 칸이 붙고 방문 쪽이 실패해도 로그의 status 와 HTTP 판정과 지표 칸은 그대로다`() {
        logCollector.runOnce()
        metricStream.add(MetricRecords.resource("1000-0", now.minusSeconds(1)))
        metricCollector.runOnce()
        val before = collectorHealth(null).report(now)
        assertTrue(before.healthy)
        assertFalse(before.body.containsKey("visits"), "방문 칸을 주지 않았는데 visits 가 있다")

        // 방문 쪽: Redis 불통 + DB 불통 + 저장 실패
        stream.reachable = false
        databaseReachable = false
        sink.failure = IllegalStateException("boom")
        collector.runOnce()
        val after = collectorHealth(visitHealth()).report(now)
        assertTrue(after.healthy, "방문 수집의 실패가 HTTP 판정을 바꿨다: ${after.body}")
        assertEquals("ok", after.body["status"])
        @Suppress("UNCHECKED_CAST")
        val visits = after.body["visits"] as Map<String, Any?>
        assertEquals("unreachable", visits["status"])
        assertEquals(before.body["metrics"], after.body["metrics"], "방문 수집의 실패가 지표 칸을 바꿨다")
        assertEquals(before.body - "metrics", after.body - "metrics" - "visits", "방문 칸 말고도 바뀐 칸이 있다")
    }

    @Test
    fun `방문 칸의 읽기가 예외를 내도 헬스는 답하고 로그의 판정은 그대로이며 그 예외의 문구는 본문에 없다`() {
        logCollector.runOnce()
        val exploding = VisitHealth(collector, unacknowledged = { throw IllegalStateException("redis $ADDRESS_V4") }, lag = { throw IllegalStateException("lag $ADDRESS_V4") },
            latestStoredAt = { throw IllegalStateException("db $ADDRESS_V4") }, lastRetention = { throw IllegalStateException("retention $ADDRESS_V4") },
            startedAt = startedAt)
        val report = collectorHealth(exploding).report(now)
        assertTrue(report.healthy, "${report.body}")
        assertTrue(report.body.containsKey("visits"), "${report.body}")
        @Suppress("UNCHECKED_CAST")
        assertNotEquals("ok", (report.body["visits"] as Map<String, Any?>)["status"], "${report.body}")
        assertFalse(report.body.toString().contains(ADDRESS_V4), "${report.body}")
    }
}
