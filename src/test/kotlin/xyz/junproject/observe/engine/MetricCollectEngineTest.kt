package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.RedisLogStreamSource
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricDropReason
import xyz.junproject.observe.metric.MetricEntryParse
import xyz.junproject.observe.metric.MetricEntryParser
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricSink
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 실제 Redis 7.4 + PostgreSQL — 명세 §11.2: 저장 뒤에만 확인 · 같은 항목(같은 스트림 ID)은 한 번만 저장 · 깨진 항목은 이유를 세고 확인 ·
 * DB 가 실패하면 확인하지 않고 다음 바퀴에 다시 · 깨진 항목이 같은 묶음의 다른 항목을 막지 않음 · 측정 시각이 2분 넘게 앞서면 버림.
 * 그리고 지표 수집이 로그 수집과 같은 Redis 에서 서로를 막지 않는가.
 * 측정 시각은 실제 지금으로 보내고, 수집기의 시계(수신 시각)는 테스트가 정한다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricCollectEngineTest {
    private val schema = "metric_collect"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        // 검색 경로에 public 을 뒤에 둔다 — 로그 표(pg_trgm)를 함께 만드는 사례가 있다
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@MetricCollectEngineTest.schema},public"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 6
    })
    private val store = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producer = EngineSupport.redisTemplate(connect())
    private lateinit var stream: String
    private val group = "observe"
    private var clock = Instant.now()

    private fun connect() = EngineSupport.redisConnection().also { connections += it }
    private fun source(connection: LettuceConnectionFactory = connect()) = RedisMetricStreamSource(EngineSupport.redisTemplate(connection), stream, group)
    private fun collector(source: RedisMetricStreamSource = source(), sink: MetricSink = store) =
        MetricCollector(source, sink, block = Duration.ofMillis(200), clock = { clock })

    /** agent 가 보내는 것과 같은 꼴의 항목을 넣는다 — 스트림 ID 를 돌려준다 */
    private fun send(measuredAt: Instant = clock.minusSeconds(1), server: String = "ci-agent-158", fields: Map<String, String> = emptyMap()): String {
        val record = MetricRecords.resource("unused", measuredAt, server)
        return producer.opsForStream<String, String>().add(stream, record.fields + fields)!!.value
    }

    private fun sendRaw(fields: Map<String, String>) = producer.opsForStream<String, String>().add(stream, fields)!!.value

    /** 저장된 항목 수 — 끝난 5분 구간을 평균 내 표본 수로 센다(표 이름을 몰라도 같은 항목이 두 번 들어갔는지 보인다) */
    private fun storedSamples(server: String = "ci-agent-158"): Long {
        val rollUpAt = clock.plus(Duration.ofMinutes(10))
        store.rollUp(rollUpAt)
        return store.rollups(server, MetricScope.HOST, null, clock.minus(Duration.ofHours(1)), rollUpAt)
            .sumOf { it.values["load1"]?.samples ?: 0L }
    }

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        stream = "observe-test-metrics-${UUID.randomUUID()}"
        clock = Instant.now()
    }

    @AfterEach fun dropStream() { producer.delete(stream) }

    @AfterAll
    fun close() {
        connections.forEach { it.destroy() }
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private class SwitchableSink(private val delegate: MetricSink) : MetricSink {
        @Volatile var failing = false
        override fun createSchema() = delegate.createSchema()
        override fun insertAll(samples: List<ResourceSample>): Int { if (failing) throw IllegalStateException("저장 실패(테스트)"); return delegate.insertAll(samples) }
    }

    @Test
    fun `읽고 저장한 뒤 확인하면 미확인이 0 이고 수신 시각은 수집기의 시계다`() {
        repeat(3) { send(measuredAt = clock.minusSeconds(10L - it)) }
        val reader = source()
        val collector = collector(reader)
        assertEquals(3, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(clock, store.latest(clock.minusSeconds(1)).single().receivedAt)
        assertEquals(3L, storedSamples())
    }

    @Test
    fun `저장하고 확인하기 전에 죽으면 다시 받지만 같은 항목은 한 번만 저장된다`() {
        repeat(3) { send() }
        val dyingConnection = connect()
        val dying = source(dyingConnection)
        dying.ensureGroup()
        val received = dying.readNew(100, Duration.ofMillis(200))
        assertEquals(3, received.size)
        // 저장까지는 했다 — 확인 전에 죽었다
        val stored = received.map { (MetricEntryParser.parse(it, clock) as MetricEntryParse.Accepted).sample }
        assertEquals(3, store.insertAll(stored))
        dyingConnection.destroy()

        val restartedSource = source()
        assertEquals(3L, restartedSource.unacknowledgedCount())
        val restarted = collector(restartedSource)
        assertEquals(3, restarted.runOnce())
        assertEquals(0L, restartedSource.unacknowledgedCount())
        assertEquals(3L, storedSamples(), "다시 받은 같은 항목이 한 번 더 저장됐다")
    }

    @Test
    fun `깨진 항목은 이유를 세고 확인해 다시 받지 않고 같은 묶음의 다른 항목은 저장된다`() {
        send()
        sendRaw(mapOf("kind" to "resource", "v" to "1", "server" to "ci-agent-158", "at" to clock.toEpochMilli().toString(), "body" to "{"))
        sendRaw(mapOf("kind" to "resource", "v" to "7", "server" to "ci-agent-158", "at" to clock.toEpochMilli().toString(), "body" to "{}"))
        sendRaw(mapOf("kind" to "resource", "v" to "1", "server" to "BAD SERVER", "at" to clock.toEpochMilli().toString(), "body" to "{}"))
        send()
        val reader = source()
        val collector = collector(reader)
        assertEquals(5, collector.runOnce())
        assertEquals(0L, reader.unacknowledgedCount(), "깨진 항목이 미확인으로 남았다")
        assertEquals(2L, storedSamples())
        val dropped = collector.droppedCounts()
        assertEquals(1L, dropped[MetricDropReason.BODY_NOT_JSON])
        assertEquals(1L, dropped[MetricDropReason.UNSUPPORTED_VERSION])
        assertEquals(1L, dropped[MetricDropReason.INVALID_SERVER])
        repeat(2) { collector.runOnce() }
        assertEquals(3L, collector.droppedCounts().values.sum(), "깨진 항목을 다시 받아 또 셌다")
    }

    @Test
    fun `DB 가 실패하면 확인하지 않아 미확인으로 남고 다음 바퀴에 다시 받아 저장한다`() {
        repeat(4) { send() }
        val reader = source()
        val sink = SwitchableSink(store)
        val collector = collector(reader, sink)
        sink.failing = true
        assertEquals(0, collector.runOnce())
        assertEquals(4L, reader.unacknowledgedCount(), "저장에 실패했는데 확인됐다")
        assertNotNull(collector.lastFailure)
        assertEquals(0L, MetricDropReason.entries.sumOf { collector.droppedCounts()[it] ?: 0L }, "DB 실패를 깨진 항목으로 셌다")
        sink.failing = false
        assertEquals(4, collector.runOnce())
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(4L, storedSamples())
    }

    @Test
    fun `측정 시각이 수신 시각보다 2분 넘게 앞서면 버리고 2분까지는 받는다`() {
        send(measuredAt = clock.plus(Duration.ofMinutes(2)))
        send(measuredAt = clock.plus(Duration.ofMinutes(2)).plusMillis(1))
        val collector = collector()
        assertEquals(2, collector.runOnce())
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.CLOCK_AHEAD])
        assertEquals(clock.plus(Duration.ofMinutes(2)), store.latest(clock.minusSeconds(1)).single().measuredAt)
    }

    @Test
    fun `다른 종류의 항목은 저장하지 않고 건너뛰어 확인한다`() {
        sendRaw(mapOf("kind" to "trace", "v" to "1", "payload" to "{\"span\":1}"))
        send()
        val reader = source()
        val collector = collector(reader)
        assertEquals(2, collector.runOnce())
        assertEquals(1L, collector.skippedOtherKind)
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(1L, storedSamples())
    }

    @Test
    fun `밀린 건수는 넣은 건수이고 수집 뒤 0 이다`() {
        val reader = source()
        repeat(6) { send() }
        reader.ensureGroup()
        assertEquals(6L, reader.lag())
        collector(reader).runOnce()
        assertEquals(0L, reader.lag())
    }

    @Test
    fun `Redis 가 스트림과 그룹을 잃어도 다시 만들고 그 뒤의 지표를 받는다`() {
        send()
        val collector = collector()
        assertEquals(1, collector.runOnce())
        producer.delete(stream)
        repeat(2) { collector.runOnce() }
        send(server = "ci-agent-164")
        repeat(3) { collector.runOnce() }
        assertNull(collector.lastFailure, "재시작 뒤에도 실패가 이어진다 — 수집이 멈췄다")
        assertEquals(setOf("ci-agent-158", "ci-agent-164"), store.latest(clock.minusSeconds(1)).map { it.server }.toSet())
    }

    @Test
    fun `상한에 밀려 칸이 사라진 미확인 항목은 trimmed 로 세고 수집이 멈추지 않는다`() {
        val reader = source()
        reader.ensureGroup()
        repeat(2) { send() }
        assertEquals(2, reader.readNew(100, Duration.ofMillis(200)).size)
        producer.opsForStream<String, String>().trim(stream, 0)
        send()
        val collector = collector(reader)
        repeat(2) { collector.runOnce() }
        assertNull(collector.lastFailure)
        assertEquals(2L, collector.droppedCounts()[MetricDropReason.TRIMMED])
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(1L, storedSamples())
    }

    @Test
    fun `지표 스트림에 쓰레기가 쌓여도 같은 Redis 의 로그 수집은 멈추지 않는다`() {
        val logStream = "observe-test-logs-${UUID.randomUUID()}"
        val logStore = EngineSupport.store(dataSource)
        logStore.createSchema()
        try {
            repeat(50) { sendRaw(mapOf("garbage" to "x".repeat(1_000))) }
            sendRaw(mapOf("kind" to "resource", "v" to "1", "server" to "ci-agent-158", "at" to "nope", "body" to "{"))
            val logCollector = LogCollector(RedisLogStreamSource(EngineSupport.redisTemplate(connect()), logStream, group), logStore, block = Duration.ofMillis(200))
            val metricCollector = collector()
            producer.opsForStream<String, String>().add(logStream, mapOf("level" to "info", "line" to "req-1:backend:hello"))
            metricCollector.runOnce()
            assertEquals(1, logCollector.runOnce())
            assertNull(logCollector.lastFailure)
            assertEquals(51L, metricCollector.droppedCounts().values.sum())
        } finally {
            producer.delete(logStream)
        }
    }
}
