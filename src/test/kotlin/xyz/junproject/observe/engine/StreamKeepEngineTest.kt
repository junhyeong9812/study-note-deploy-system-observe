package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.domain.Range
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.RedisLogStreamSource
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.RedisMetricStreamSource
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 특성 테스트(V2b 전에도 초록) — 로그 · 지표 수집은 저장하고 확인한 항목을 **스트림에서 지우지 않는다**. 방문 스트림에만 켜지는 지우기가
 * 다른 스트림으로 번지지 않는지를 고정한다(같은 스트림 읽기 코드 `RedisLogStreamSource` · `RedisMetricStreamSource` 를 함께 쓴다).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StreamKeepEngineTest {
    private val schema = "stream_keep"
    private val administrationSource = EngineSupport.dataSource(size = 1)
    private val administration = JdbcTemplate(administrationSource)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@StreamKeepEngineTest.schema},public"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 3
    })
    private val connection = EngineSupport.redisConnection()
    private val redis = EngineSupport.redisTemplate(connection)

    @AfterAll
    fun close() {
        connection.destroy()
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administrationSource.close()
    }

    @Test
    fun `로그 · 지표 수집은 저장하고 확인한 항목을 스트림에서 지우지 않는다`() {
        administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        val logStream = "observe-test-keep-logs-${UUID.randomUUID()}"
        val metricStream = "observe-test-keep-metrics-${UUID.randomUUID()}"
        try {
            val streams = redis.opsForStream<String, String>()
            val logIds = List(3) { streams.add(logStream, mapOf("level" to "info", "line" to "req-keep-$it:backend:hello"))!!.value }
            val metricIds = List(2) { streams.add(metricStream, MetricRecords.resource("unused", Instant.now().minusSeconds(5L - it)).fields)!!.value }
            val logSource = RedisLogStreamSource(redis, logStream, "observe")
            val logCollector = LogCollector(logSource, EngineSupport.store(dataSource).also { it.createSchema() }, block = Duration.ofMillis(200))
            val metricSource = RedisMetricStreamSource(redis, metricStream, "observe")
            val metricCollector = MetricCollector(metricSource, JdbcTemplate(dataSource).let { MetricStore(it, it, it) }, block = Duration.ofMillis(200))
            assertEquals(3, logCollector.runOnce())
            assertEquals(2, metricCollector.runOnce())
            assertEquals(0L, logSource.unacknowledgedCount())
            assertEquals(0L, metricSource.unacknowledgedCount())
            assertEquals(logIds, streams.range(logStream, Range.unbounded<String>())!!.map { it.id.value }, "로그 스트림의 항목이 지워졌다")
            assertEquals(metricIds, streams.range(metricStream, Range.unbounded<String>())!!.map { it.id.value }, "지표 스트림의 항목이 지워졌다")
        } finally {
            redis.delete(logStream)
            redis.delete(metricStream)
        }
    }
}
