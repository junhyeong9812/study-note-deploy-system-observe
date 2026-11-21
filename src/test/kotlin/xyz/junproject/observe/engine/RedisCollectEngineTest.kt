package xyz.junproject.observe.engine

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
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogSink
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 실제 Redis 7.4 + PostgreSQL — load-bearing 가정 1 의 실증:
 * Spring Data Redis 로 소비자 그룹 읽기(블로킹)·ack·미확인분 재수신이 기대대로 동작하는가.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RedisCollectEngineTest {
    private val dataSource: HikariDataSource = EngineSupport.dataSource(size = 6)
    private val store = EngineSupport.store(dataSource)
    private val jdbc = JdbcTemplate(dataSource)
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producer = EngineSupport.redisTemplate(connect())
    private lateinit var stream: String
    private val group = "observe"

    private fun connect() = EngineSupport.redisConnection().also { connections += it }
    private fun source(connection: LettuceConnectionFactory = connect()) = RedisLogStreamSource(EngineSupport.redisTemplate(connection), stream, group)
    private fun send(line: String, level: String = "info") = producer.opsForStream<String, String>().add(stream, mapOf("level" to level, "line" to line))!!.value
    private fun rowCount() = jdbc.queryForObject("SELECT count(*) FROM log_entry", Long::class.java)!!
    private fun storedIds() = jdbc.queryForList("SELECT stream_ms || '-' || stream_seq FROM log_entry", String::class.java).toSet()

    @BeforeEach fun reset() { EngineSupport.resetTable(dataSource); stream = "observe-test-${UUID.randomUUID()}" }
    @AfterEach fun dropStream() { producer.delete(stream) }
    @AfterAll fun close() { connections.forEach { it.destroy() }; dataSource.close() }

    /** 실패하라고 할 때 실패하는 저장소 */
    private class SwitchableSink(private val delegate: LogSink) : LogSink {
        @Volatile var failing = false
        override fun insertAll(entries: List<LogEntry>): Int { if (failing) throw IllegalStateException("저장 실패(테스트)"); return delegate.insertAll(entries) }
    }

    @Test
    fun `읽고 저장한 뒤 확인하면 미확인이 0 이다`() {
        val sent = (1..5).map { send("req-$it:backend:hello $it") }.toSet()
        val reader = source()
        val collector = LogCollector(reader, store, block = Duration.ofMillis(200))
        assertEquals(5, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(sent, storedIds())
        assertEquals(0L, reader.unacknowledgedCount())
    }

    @Test
    fun `저장에 실패하면 확인하지 않아 미확인으로 남고 다음 바퀴에 다시 받아 저장한다`() {
        val sent = (1..5).map { send("req-$it:backend:hello $it") }.toSet()
        val reader = source()
        val sink = SwitchableSink(store)
        val collector = LogCollector(reader, sink, block = Duration.ofMillis(200))
        sink.failing = true
        assertEquals(0, collector.runOnce())
        assertEquals(5L, reader.unacknowledgedCount(), "저장에 실패했는데 확인됐다")
        assertEquals(0L, rowCount())
        sink.failing = false
        assertEquals(5, collector.runOnce())
        assertEquals(sent, storedIds())
        assertEquals(0L, reader.unacknowledgedCount())
    }

    @Test
    fun `저장하기 전에 죽은 뒤 다시 뜬 수집기가 미확인분부터 받는다`() {
        val sentBefore = (1..5).map { send("req-$it:backend:before $it") }
        val dyingConnection = connect()
        val dying = source(dyingConnection)
        dying.ensureGroup()
        assertEquals(sentBefore, dying.readNew(100, Duration.ofMillis(200)).map { it.id })   // 받았지만 저장·확인 전에
        dyingConnection.destroy()                                                           // 프로세스가 죽었다

        val sentWhileDown = (6..8).map { send("req-$it:backend:while down $it") }
        val restartedSource = source()
        assertEquals(5L, restartedSource.unacknowledgedCount())
        val restarted = LogCollector(restartedSource, store, block = Duration.ofMillis(200))
        assertEquals(8, restarted.runOnce())
        assertEquals((sentBefore + sentWhileDown).toSet(), storedIds())
        assertEquals(0L, restartedSource.unacknowledgedCount())
    }

    @Test
    fun `수집기 둘이 동시에 읽어도 넣은 건수와 저장된 건수가 같다`() {
        val total = 5_000
        val sinkOfFirst = SwitchableSink(store)
        val collectors = listOf(
            LogCollector(source(), sinkOfFirst, batchSize = 50, block = Duration.ofMillis(100)),
            LogCollector(source(), store, batchSize = 50, block = Duration.ofMillis(100)),
        )
        val handled = collectors.map { AtomicInteger() }
        val running = AtomicBoolean(true)
        val rounds = AtomicInteger()
        val workers = collectors.mapIndexed { index, collector ->
            thread(name = "observe-test-collector-$index") {
                while (running.get()) {
                    // 첫 수집기는 일곱 바퀴에 한 번 저장에 실패한다 — 동시에 돌며 실패·재수신이 섞여도 잃지 않는가
                    if (index == 0) sinkOfFirst.failing = rounds.incrementAndGet() % 7 == 0
                    handled[index].addAndGet(collector.runOnce())
                }
            }
        }
        val sent = ConcurrentHashMap.newKeySet<String>()
        repeat(total) { sent += send("req-$it:front:concurrent $it") }
        sinkOfFirst.failing = false
        val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos()
        val checker = source()
        while ((rowCount() < total || checker.unacknowledgedCount() > 0) && System.nanoTime() < deadline) Thread.sleep(100)
        running.set(false)
        workers.forEach { it.join(10_000) }

        assertEquals(total, sent.size)
        assertEquals(total.toLong(), rowCount(), "넣은 건수와 저장된 건수가 다르다")
        assertEquals(sent, storedIds())
        assertEquals(0L, checker.unacknowledgedCount())
        assertTrue(handled.all { it.get() > 0 }, "두 수집기가 모두 받아야 동시 읽기를 본 것이다: ${handled.map { it.get() }}")
    }

    @Test
    fun `Redis 가 스트림과 그룹을 잃어도 다시 만들고 그 뒤의 로그를 받는다`() {
        (1..3).forEach { send("req-$it:backend:before restart $it") }
        val collector = LogCollector(source(), store, block = Duration.ofMillis(200))
        assertEquals(3, collector.runOnce())
        producer.delete(stream)                       // 디스크 저장 없는 Redis 의 재시작 — 스트림·그룹이 함께 사라진다
        repeat(2) { collector.runOnce() }             // 그 사이 수집은 실패할 수 있다
        val sentAfter = (4..5).map { send("req-$it:backend:after restart $it") }
        repeat(3) { collector.runOnce() }
        assertNull(collector.lastFailure, "재시작 뒤에도 실패가 이어진다 — 수집이 멈췄다")
        assertTrue(storedIds().containsAll(sentAfter), "재시작 뒤의 로그를 받지 못했다")
    }

    @Test
    fun `확인하지 못한 항목이 상한에 밀려 사라져도 수집이 멈추지 않고 빈 행을 만들지 않는다`() {
        val reader = source()
        reader.ensureGroup()
        (1..3).forEach { send("req-$it:backend:will be trimmed $it") }
        assertEquals(3, reader.readNew(100, Duration.ofMillis(200)).size)     // 받았지만 확인하지 못했다
        producer.opsForStream<String, String>().trim(stream, 0)              // 스트림 상한에 밀려났다(명세 ②: 받아들이는 유실)
        val sentAfter = (4..5).map { send("req-$it:backend:kept $it") }.toSet()
        val collector = LogCollector(reader, store, block = Duration.ofMillis(200))
        repeat(3) { collector.runOnce() }
        assertNull(collector.lastFailure, "사라진 미확인 항목 때문에 수집이 실패한다")
        assertEquals(sentAfter, storedIds(), "사라진 항목을 빈 줄로 저장했거나 새 항목을 놓쳤다")
        assertEquals(0L, reader.unacknowledgedCount(), "사라진 미확인 항목이 영영 미확인으로 남는다")
    }

    @Test
    fun `블로킹 읽기가 기다리는 동안에도 미확인 건수 조회는 바로 답한다`() {
        val reader = source()
        reader.ensureGroup()
        val blocking = thread { reader.readNew(10, Duration.ofSeconds(3)) }
        Thread.sleep(300)
        val started = System.nanoTime()
        reader.unacknowledgedCount()
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        blocking.join()
        assertTrue(elapsed < Duration.ofSeconds(1), "헬스가 블로킹 읽기에 묶였다: $elapsed")
    }
}
