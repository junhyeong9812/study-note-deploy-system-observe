package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RuleState
import xyz.junproject.observe.threshold.JudgeInput
import xyz.junproject.observe.threshold.ThresholdJudge
import java.time.Duration
import java.time.Instant

/**
 * 명세 §16 · §16.1 (리뷰 루프 2) — 값의 상한(초당 바이트 1e13 · 부하 1e6 · 컨테이너 CPU 1e6, 바이트 수는 상한 없음), 헬스의 지표 칸은 로그 쪽과 동시에 읽고
 * 상태 읽기와 선택 칸 읽기가 서로를 기다리지 않음, 5분 평균의 실패 구간 수가 헬스에, 재시작 해소는 컨테이너 갈래가 온전한 표본의 ID 집합으로.
 */
class MetricValueLimitTest {
    private val receivedAt = Instant.parse("2026-11-24T03:00:00Z")

    private fun parse(body: String) = MetricEntryParser.parse(MetricRecords.resource("1-0", receivedAt.minusSeconds(1), body = body), receivedAt)
    private fun host(field: String, value: String) = MetricRecords.body().replace(Regex("\"$field\": [^,}]+"), "\"$field\": $value")
    private fun containerCpu(value: String) = MetricRecords.body(containers = """[{"name": "front-app-green", "id": "0123456789ab", "project": "p", "cpu_pct": $value, "mem_used": 1, "restarts": 0, "state": "running"}]""")

    @Test
    fun `초당 바이트는 1e13 까지 받고 넘으면 invalid_value — 100GbE 수준은 받는다`() {
        for (field in listOf("net_rx_bps", "net_tx_bps", "io_read_bps", "io_write_bps")) {
            assertTrue(parse(host(field, "10000000000000")) is MetricEntryParse.Accepted, "$field 1e13")
            assertTrue(parse(host(field, "12500000000")) is MetricEntryParse.Accepted, "$field 1.25e10")
            assertEquals(MetricEntryParse.Dropped(MetricDropReason.INVALID_VALUE), parse(host(field, "10000000000001")), "$field 1e13 + 1")
        }
    }

    @Test
    fun `부하는 1e6 · 컨테이너 CPU 는 1e6 까지 받고 넘으면 invalid_value`() {
        assertTrue(parse(host("load1", "1000000")) is MetricEntryParse.Accepted)
        assertEquals(MetricEntryParse.Dropped(MetricDropReason.INVALID_VALUE), parse(host("load1", "1000000.5")))
        assertTrue(parse(containerCpu("1000000")) is MetricEntryParse.Accepted)
        assertEquals(MetricEntryParse.Dropped(MetricDropReason.INVALID_VALUE), parse(containerCpu("1000001")))
    }

    @Test
    fun `바이트 수에는 상한이 없다 — 2^62 바이트 디스크를 받는다`() {
        val huge = "4611686018427387904"
        val sample = (parse(host("disk_total", huge).let { Regex("\"disk_used\": [^,}]+").replace(it, "\"disk_used\": $huge") }) as? MetricEntryParse.Accepted)?.sample
            ?: fail("2^62 바이트를 버렸다")
        assertEquals(4_611_686_018_427_387_904L, sample.host!!.diskTotal)
    }
}

class MetricHealthDeadlineTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val stream = FakeMetricStream()
    private val collector = MetricCollector(stream, FakeMetricSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }

    private fun health(latestStoredAt: () -> Instant? = { now }, receivedByServer: () -> Map<String, Instant> = { mapOf("ci-agent-158" to now) },
                       rollup: MetricRollupResult? = null) = MetricHealth(
        collector, { stream.unacknowledgedCount() }, { 0L }, latestStoredAt, receivedByServer, { rollup }, { null }, { null }, startedAt = now)

    private fun <T> timed(block: () -> T): Pair<T, Duration> { val started = System.nanoTime(); val value = block(); return value to Duration.ofNanos(System.nanoTime() - started) }

    @Test
    fun `상태를 정하는 읽기가 5초 걸리면 지표 칸은 2초대에 unreachable 로 돌아온다`() {
        val (body, elapsed) = timed { health(latestStoredAt = { Thread.sleep(5_000); now }).report(now) }
        assertEquals("unreachable", body["status"])
        assertTrue(elapsed < Duration.ofMillis(3_000), "$elapsed")
    }

    @Test
    fun `선택 칸의 읽기만 5초 걸리면 status 는 그대로이고 그 칸만 비며 세 번 잇달아도 같다`() {
        val slow = health(receivedByServer = { Thread.sleep(5_000); mapOf("ci-agent-158" to now) })
        repeat(3) {
            val (body, elapsed) = timed { slow.report(now) }
            assertEquals("ok", body["status"], "선택 칸이 막혀 상태가 바뀌었다(${it + 1}번째)")
            assertEquals(emptyMap<String, String>(), body["last_received_at"])
            assertTrue(elapsed < Duration.ofMillis(3_000), "$elapsed")
        }
    }

    @Test
    fun `지표 칸의 읽기는 로그 쪽 읽기와 동시에 시작한다 — 둘 다 1초 반이면 전체가 2초대`() {
        val logStream = FakeLogStream()
        val logCollector = LogCollector(logStream, FakeLogSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        val collectorHealth = CollectorHealth(logCollector, { logStream.unacknowledgedCount() }, { 0L }, { Thread.sleep(1_500); null }, { null },
            startedAt = now, metrics = health(latestStoredAt = { Thread.sleep(1_500); now }))
        val (report, elapsed) = timed { collectorHealth.report(now) }
        assertEquals("ok", report.body["status"])
        @Suppress("UNCHECKED_CAST")
        assertEquals("ok", (report.body["metrics"] as Map<String, Any?>)["status"])
        assertTrue(elapsed < Duration.ofMillis(2_800), "로그 쪽을 다 읽은 뒤에 지표를 읽었다: $elapsed")
    }

    @Test
    fun `5분 평균에서 계산에 실패한 구간 수가 헬스의 지표 칸에 나온다`() {
        assertEquals(-1L, (health().report(now)["last_rollup_failed_buckets"] as Number).toLong())
        val body = health(rollup = MetricRollupResult(now, 7, null, failedBuckets = 2)).report(now)
        assertEquals(2L, (body["last_rollup_failed_buckets"] as Number).toLong())
    }

    @Test
    fun `예약된 5분 평균은 구간마다 따로 계산하는 면을 부르고 실패 구간 수를 남긴다`() {
        val maintenance = object : MetricMaintenance {
            override fun rollUp(now: Instant): Int = fail("구간마다 따로 계산하는 rollUpBuckets 를 불러야 한다")
            override fun rollUpBuckets(now: Instant) = RollupOutcome(5, 2)
            override fun deleteExpired(now: Instant) = MetricDeletion(0, 0)
        }
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, metrics = maintenance)
        assertEquals(5, schedule.rollUpMetrics(now))
        assertEquals(MetricRollupResult(now, 5, null, failedBuckets = 2), schedule.lastMetricRollup)
    }

    private val unreachableLogStore = org.springframework.jdbc.core.JdbcTemplate(object : org.springframework.jdbc.datasource.AbstractDataSource() {
        override fun getConnection(): java.sql.Connection = throw java.sql.SQLException("Connection refused")
        override fun getConnection(username: String?, password: String?): java.sql.Connection = getConnection()
    }).let { xyz.junproject.observe.log.LogStore(it, it, it) }
}

class RestartResolveIdSetTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private var sequence = 0

    private fun container(id: String, name: String) = ContainerValues(name, id, "study-note-deploy-system-front", 1.0, 1, 4, "running")

    @Test
    fun `오래된 절반은 잘린 목록이고 그중에 울리던 ID 가 있어도 최근 절반이 온전하고 그 ID 가 없으면 해소한다`() {
        val firing = RuleState("ci-agent-158", ResourceRule.RESTARTS, "aaaaaaaaaaaa", "front-app-green", true,
            now.minus(Duration.ofMinutes(20)), now.minus(Duration.ofMinutes(20)), 4.0)
        val samples = (19 downTo 0).mapIndexed { index, ago ->
            val at = now.minusSeconds(30L * ago)
            val (containers, missing) = if (index < 10) listOf(container("aaaaaaaaaaaa", "front-app-green")) to listOf("containers_truncated")
                else listOf(container("bbbbbbbbbbbb", "front-app-blue")) to emptyList()
            ResourceSample(StreamId(at.toEpochMilli(), sequence++), "ci-agent-158", at, at, 30_000,
                HostValues(cpuPct = 1.0, load1 = 0.1, memTotal = 100, memUsed = 10, diskTotal = 100, diskUsed = 10), containers, emptyList(), missing)
        }
        val result = ThresholdJudge().judge(JudgeInput(now, samples, mapOf("ci-agent-158" to now), emptySet()), listOf(firing))
        assertEquals(listOf("resource:ci-agent-158:restarts:front-app-green:resolved"), result.events.map { it.key },
            "잘린 목록의 ID 를 '아직 있음'으로 셌다")
    }
}
