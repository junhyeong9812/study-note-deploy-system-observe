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
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.traffic.TrafficCellQuery
import xyz.junproject.observe.traffic.TrafficCollector
import xyz.junproject.observe.traffic.TrafficDropReason
import xyz.junproject.observe.traffic.TrafficEntry
import xyz.junproject.observe.traffic.TrafficEntryParse
import xyz.junproject.observe.traffic.TrafficEntryParser
import xyz.junproject.observe.traffic.TrafficRecords
import xyz.junproject.observe.traffic.TrafficSink
import xyz.junproject.observe.traffic.TrafficStore
import xyz.junproject.observe.traffic.TrafficStoreOutcome
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 실제 Redis 7.4 + PostgreSQL — 규약 4.1 · 4.5: 따로의 스트림을 지표와 같은 수집 코드(소비자 그룹 · 저장 뒤 확인)로 받는다.
 * 저장한 뒤에만 확인 · 저장이 실패하면 확인하지 않아 다시 받음 · 저장한 뒤 확인 전에 죽으면 다시 받아 순번으로 버림 · 깨진 항목은 이유를 세고 확인 ·
 * 통신 스트림의 쓰레기가 같은 Redis 의 로그 · 지표 수집을 막지 않음. 표는 전용 스키마(`traffic_collect`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TrafficCollectEngineTest {
    private val schema = "traffic_collect"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@TrafficCollectEngineTest.schema},public"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 6
    })
    private val store = JdbcTemplate(dataSource).let { TrafficStore(it, it, it) }
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producer = EngineSupport.redisTemplate(connect())
    private lateinit var stream: String
    private val group = "observe"
    private var clock = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    private fun connect() = EngineSupport.redisConnection().also { connections += it }
    private fun source(connection: LettuceConnectionFactory = connect()) = RedisMetricStreamSource(EngineSupport.redisTemplate(connection), stream, group)
    private fun collector(source: RedisMetricStreamSource = source(), sink: TrafficSink = store) =
        TrafficCollector(source, sink, block = Duration.ofMillis(200), clock = { clock })

    private val cell get() = TrafficRecords.cellStart(clock).minusSeconds(30)

    /** 에이전트가 보내는 것과 같은 꼴 — 칸 하나에 요청 [count] 개 */
    private fun send(sequence: Long, count: Int = 2, generation: String = TrafficRecords.GENERATION): String {
        val body = TrafficRecords.body(listOf(TrafficRecords.segment(cell, List(count) { 200 })))
        return producer.opsForStream<String, String>().add(stream, TrafficRecords.fields(sequence, clock.minusSeconds(1), body, generation = generation))!!.value
    }

    private fun sendRaw(fields: Map<String, String>) = producer.opsForStream<String, String>().add(stream, fields)!!.value

    private fun storedCount(): Long = store.cells(TrafficCellQuery(cell, clock.plusSeconds(60), cell)).sumOf { it.totals.count }

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        stream = "observe-test-traffic-${UUID.randomUUID()}"
        clock = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    }

    @AfterEach fun dropStream() { producer.delete(stream) }

    @AfterAll
    fun close() {
        connections.forEach { it.destroy() }
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private class SwitchableSink(private val delegate: TrafficSink) : TrafficSink {
        @Volatile var failing = false
        override fun createSchema() = delegate.createSchema()
        override fun store(entry: TrafficEntry): TrafficStoreOutcome { if (failing) throw IllegalStateException("저장 실패(테스트)"); return delegate.store(entry) }
    }

    @Test
    fun `읽고 저장한 뒤 확인하면 미확인이 0 이고 받은 시각은 수집기의 시계다`() {
        repeat(3) { send(it + 1L) }
        val reader = source()
        val collector = collector(reader)
        assertEquals(3, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(6L, storedCount())
        assertEquals(clock, store.serverProgress().single().lastReceivedAt)
        assertEquals(0L, store.serverProgress().single().sequenceGaps)
    }

    @Test
    fun `저장한 뒤 확인하기 전에 죽으면 다시 받지만 순번으로 버려 칸은 한 번만 더해진다`() {
        repeat(3) { send(it + 1L) }
        val dyingConnection = connect()
        val dying = source(dyingConnection)
        dying.ensureGroup()
        val received = dying.readNew(100, Duration.ofMillis(200))
        assertEquals(3, received.size)
        received.forEach { store.store((TrafficEntryParser.parse(it, clock) as TrafficEntryParse.Accepted).entry) }
        dyingConnection.destroy()

        val restartedSource = source()
        assertEquals(3L, restartedSource.unacknowledgedCount())
        val restarted = collector(restartedSource)
        assertEquals(3, restarted.runOnce())
        assertEquals(0L, restartedSource.unacknowledgedCount())
        assertEquals(3L, restarted.duplicates)
        assertEquals(6L, storedCount(), "다시 받은 같은 항목의 칸이 한 번 더 더해졌다")
    }

    @Test
    fun `저장이 실패하면 확인하지 않아 미확인으로 남고 다음 바퀴에 다시 받아 저장한다`() {
        repeat(4) { send(it + 1L) }
        val reader = source()
        val sink = SwitchableSink(store)
        val collector = collector(reader, sink)
        sink.failing = true
        assertEquals(0, collector.runOnce())
        assertEquals(4L, reader.unacknowledgedCount(), "저장에 실패했는데 확인됐다")
        assertEquals("IllegalStateException", collector.lastStoreFailure)
        sink.failing = false
        assertEquals(4, collector.runOnce())
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(8L, storedCount())
        assertEquals(0L, collector.duplicates)
    }

    @Test
    fun `깨진 항목 · 칸이 사라진 항목은 이유를 세고 확인하고 수집은 멈추지 않는다`() {
        val reader = source()
        reader.ensureGroup()
        send(1)
        assertEquals(1, reader.readNew(100, Duration.ofMillis(200)).size)
        producer.opsForStream<String, String>().trim(stream, 0)
        sendRaw(TrafficRecords.fields(2, clock, "{"))
        sendRaw(TrafficRecords.fields(3, clock) + ("generation" to "XYZ"))
        send(4)
        val collector = collector(reader)
        repeat(2) { collector.runOnce() }
        assertNull(collector.lastFailure)
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.TRIMMED])
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.BODY_NOT_JSON])
        assertEquals(1L, collector.droppedCounts()[TrafficDropReason.INVALID_GENERATION])
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(2L, storedCount())
        // 이 세대에서 처음 저장된 순번이 4 다 — 관측 서버가 시작을 보지 못한 것으로 드러나고(1~3 은 틈이 아니다), 버린 이유는 위에서 센다
        assertEquals(4L, store.serverProgress().single().firstSeqSeen)
        assertEquals(0L, store.serverProgress().single().sequenceGaps)
    }

    @Test
    fun `통신 스트림에 쓰레기가 쌓여도 같은 Redis 의 로그 · 지표 수집은 멈추지 않는다`() {
        val logStream = "observe-test-logs-${UUID.randomUUID()}"
        val metricStream = "observe-test-metrics-${UUID.randomUUID()}"
        val logStore = EngineSupport.store(dataSource).also { it.createSchema() }
        val metricStore = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }.also { it.createSchema() }
        try {
            repeat(50) { sendRaw(mapOf("garbage" to "x".repeat(1_000))) }
            val trafficCollector = collector()
            val logCollector = LogCollector(RedisLogStreamSource(EngineSupport.redisTemplate(connect()), logStream, group), logStore, block = Duration.ofMillis(200))
            val metricCollector = MetricCollector(RedisMetricStreamSource(EngineSupport.redisTemplate(connect()), metricStream, group), metricStore, block = Duration.ofMillis(200))
            producer.opsForStream<String, String>().add(logStream, mapOf("level" to "info", "line" to "req-1:backend:hello"))
            producer.opsForStream<String, String>().add(metricStream, MetricRecords.resource("unused", Instant.now().minusSeconds(1)).fields)
            trafficCollector.runOnce()
            assertEquals(1, logCollector.runOnce())
            assertEquals(1, metricCollector.runOnce())
            assertNull(logCollector.lastFailure)
            assertNull(metricCollector.lastFailure)
            assertEquals(50L, trafficCollector.droppedCounts().values.sum())
            assertEquals(0L, metricCollector.droppedCounts().values.sum(), "통신 항목이 지표 수집에 섞였다")
        } finally {
            producer.delete(logStream)
            producer.delete(metricStream)
        }
    }
}
