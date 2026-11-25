package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant

/**
 * 실제 PostgreSQL — 명세 §15 (리뷰 루프 1): 기대 서버가 아닌 서버가 끊긴 채 7일이 지나(원본이 지워져도) 끊김 판정 대상이고 1시간마다 다시 알린다,
 * 헬스는 판정 · 알림 표를 만들지 않는다(표가 없으면 그 칸은 "읽지 못함"). 표는 전용 스키마에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThresholdReviewEngineTest {
    private val schema = "threshold_review"
    private val emptySchema = "threshold_review_empty"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private fun pool(schemaName: String) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=$schemaName"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 3
    })
    private val dataSource = pool(schema)
    private val emptyDataSource = pool(emptySchema)
    private val metricStore = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val thresholdStore = ThresholdStore(JdbcTemplate(dataSource))
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    @BeforeEach
    fun reset() {
        for (name in listOf(schema, emptySchema)) {
            administrationJdbc.execute("DROP SCHEMA IF EXISTS $name CASCADE")
            administrationJdbc.execute("CREATE SCHEMA $name")
        }
        metricStore.createSchema()
        thresholdStore.createSchema()
    }

    @AfterAll
    fun close() {
        for (name in listOf(schema, emptySchema)) administrationJdbc.execute("DROP SCHEMA IF EXISTS $name CASCADE")
        dataSource.close()
        emptyDataSource.close()
        administration.close()
    }

    private class RecordingSender : AlertSender {
        val keys = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { keys += alert.key; return SendOutcome.Delivered }
    }

    @Test
    fun `기대 서버가 아닌 서버가 끊긴 채 7일이 지나 원본이 지워져도 끊김으로 판정하고 1시간마다 다시 알린다`() {
        val lastSeen = now.minus(Duration.ofDays(8))
        metricStore.insertAll(listOf(ResourceSample(StreamId(lastSeen.toEpochMilli(), 0), "ci-agent-9", lastSeen, lastSeen, 30_000,
            HostValues(cpuPct = 1.0), emptyList(), emptyList(), emptyList())))
        metricStore.rollUp(now)
        assertEquals(1, metricStore.deleteExpired(now).rawDeleted)
        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, thresholdStore, ThresholdJudge(), AlertDispatcher(thresholdStore, sender), thresholdStore, emptySet())
        round.runOnce(now)
        round.runOnce(now.plus(Duration.ofMinutes(30)))
        round.runOnce(now.plus(Duration.ofHours(1)))
        assertEquals(listOf("resource:ci-agent-9:silent", "resource:ci-agent-9:silent"), sender.keys,
            "원본이 지워진 서버를 잊었거나 1시간 다시 알림이 없다")
    }

    @Test
    fun `헬스는 표가 없을 때 표를 만들지 않고 그 칸은 읽지 못함(-1)이다`() {
        val emptyStore = ThresholdStore(JdbcTemplate(emptyDataSource))
        val emptyMetrics = JdbcTemplate(emptyDataSource).let { MetricStore(it, it, it) }
        val round = ThresholdRound(emptyMetrics, emptyStore, ThresholdJudge(), AlertDispatcher(emptyStore, RecordingSender()), emptyStore, emptySet())
        val stream = FakeMetricStream()
        val collector = MetricCollector(stream, FakeMetricSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        val body = MetricHealth(collector, { 0L }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null }, startedAt = now, thresholds = round).report(now)
        assertEquals(-1L, (body["undelivered_alerts"] as Number).toLong())
        assertEquals(-1L, (body["rejected_alerts"] as Number).toLong())
        val tables = administrationJdbc.queryForObject("SELECT count(*) FROM pg_tables WHERE schemaname = ?", Long::class.java, emptySchema)
        assertEquals(0L, tables, "인증 없는 헬스가 표를 만들었다")
        assertEquals("ok", body["status"])
    }
}
