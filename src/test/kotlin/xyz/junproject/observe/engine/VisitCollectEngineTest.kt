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
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.visit.CapturedLogs
import xyz.junproject.observe.visit.Visit
import xyz.junproject.observe.visit.VisitCollector
import xyz.junproject.observe.visit.VisitDropReason
import xyz.junproject.observe.visit.VisitEntryParse
import xyz.junproject.observe.visit.VisitEntryParser
import xyz.junproject.observe.visit.VisitHealth
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.SEARCH_WORDS
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import xyz.junproject.observe.visit.VisitSink
import xyz.junproject.observe.visit.VisitStore
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 실제 Redis 7.4 + PostgreSQL — 스트림 `visits` 의 소비자 그룹 수집: 저장 뒤에만 확인 · 같은 항목은 한 줄 · 꼴이 틀린 항목은 세고 확인 ·
 * 죽었다 살아나면 이어 읽기 · DB 가 죽은 동안 확인하지 않고 기다림(잃지 않음) · 저장 실패의 로그에 주소 원문이 없음.
 * 읽기는 지표의 스트림 읽기([RedisMetricStreamSource] — 칸 전부를 넘긴다)를 스트림 이름만 바꿔 쓴다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VisitCollectEngineTest {
    private val schema = "visit_collect"
    private val administration = JdbcTemplate(EngineSupport.dataSource(size = 1))
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@VisitCollectEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 3
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = VisitStore(jdbc, jdbc, jdbc)
    /** 닿지 않는 DB — 연결을 얻다가 1초 안에 실패한다 */
    private val deadDataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = "jdbc:postgresql://127.0.0.1:1/observe"; username = "observe"; password = "unused"; maximumPoolSize = 1
        connectionTimeout = 1_000; initializationFailTimeout = -1
    })
    private val deadStore = JdbcTemplate(deadDataSource).let { VisitStore(it, it, it) }
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producer = EngineSupport.redisTemplate(connect())
    private lateinit var stream: String
    private val group = "observe"

    private fun connect() = EngineSupport.redisConnection().also { connections += it }
    private fun source(connection: LettuceConnectionFactory = connect()) = RedisMetricStreamSource(EngineSupport.redisTemplate(connection), stream, group)
    private fun collector(source: RedisMetricStreamSource = source(), sink: VisitSink = store) = VisitCollector(source, sink, block = Duration.ofMillis(200))

    private fun send(fields: Map<String, String> = VisitRecords.fields()) = producer.opsForStream<String, String>().add(stream, fields)!!.value

    private fun rows(): List<Map<String, Any?>> =
        jdbc.queryForList("SELECT stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip FROM visit_event ORDER BY stream_ms, stream_seq")

    private fun streamId(row: Map<String, Any?>) = "${row["stream_ms"]}-${row["stream_seq"]}"

    /** 저장이 켜졌다 꺼졌다 하는 길 — 꺼진 동안은 닿지 않는 DB 로 간다(실제 연결 실패) */
    private inner class SwitchableSink : VisitSink {
        @Volatile var databaseDown = false
        override fun createSchema() = (if (databaseDown) deadStore else store).createSchema()
        override fun insertAll(visits: List<Visit>) = (if (databaseDown) deadStore else store).insertAll(visits)
    }

    @BeforeEach
    fun reset() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        store.createSchema()
        stream = "observe-test-visits-${UUID.randomUUID()}"
    }

    @AfterEach fun dropStream() { producer.delete(stream) }

    @AfterAll
    fun close() {
        connections.forEach { it.destroy() }
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        deadDataSource.close()
        (administration.dataSource as HikariDataSource).close()
    }

    @Test
    fun `읽고 저장한 뒤 확인하면 미확인이 0 이고 줄 수와 칸 값이 넣은 그대로이며 방문 시각은 스트림 ID 의 시각이다`() {
        val first = send()
        val second = send(VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search", referrerHost = null, device = "mobile", ip = ADDRESS_V6))
        val reader = source()
        val collector = collector(reader)
        assertEquals(2, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(0L, reader.unacknowledgedCount())
        val stored = rows()
        assertEquals(listOf(first, second), stored.map(::streamId))
        assertEquals(mapOf("path" to "/wiki/kubernetes/pod", "screen" to "topic", "referrer_host" to "www.google.com", "device" to "desktop",
            "visitor" to VISITOR_ONE, "ip" to ADDRESS_V4), stored[0] - "stream_ms" - "stream_seq")
        assertEquals(mapOf("path" to "/search", "screen" to "search", "referrer_host" to null, "device" to "mobile", "visitor" to VISITOR_ONE, "ip" to ADDRESS_V6),
            stored[1] - "stream_ms" - "stream_seq")
        assertEquals(first.substringBefore('-').toLong(), stored[0]["stream_ms"])
        assertEquals(1L, collector.queryStripped)
        assertFalse(jdbc.queryForObject("SELECT count(*) FROM visit_event WHERE path LIKE ?", Long::class.java, "%$SEARCH_WORDS%")!! > 0, "검색어가 저장됐다")
    }

    @Test
    fun `저장하고 확인하기 전에 죽으면 다시 받지만 같은 항목은 한 줄만 남는다`() {
        repeat(3) { send() }
        val dyingConnection = connect()
        val dying = source(dyingConnection)
        dying.ensureGroup()
        val delivered = dying.readNew(100, Duration.ofMillis(200))
        assertEquals(3, delivered.size)
        // 앞 프로세스가 같은 항목을 저장까지 했다 — 확인 전에 죽었다
        store.insertAll(delivered.map { record -> (VisitEntryParser.parse(record) as VisitEntryParse.Accepted).visit })
        dyingConnection.destroy()

        val restartedSource = source()
        assertEquals(3L, restartedSource.unacknowledgedCount())
        val restarted = collector(restartedSource)
        assertEquals(3, restarted.runOnce())
        assertEquals(0L, restartedSource.unacknowledgedCount())
        assertEquals(3, rows().size, "다시 받은 같은 항목이 한 줄 더 저장됐다")
    }

    @Test
    fun `수집이 죽었다 살아나면 그사이 들어온 것부터 이어 읽고 빠짐도 겹침도 없다`() {
        val before = (1..3).map { send() }
        val firstConnection = connect()
        assertEquals(3, collector(source(firstConnection)).runOnce())
        firstConnection.destroy()                                         // 수집이 죽었다
        val during = (1..4).map { send() }                                // 죽은 동안 들어왔다
        val revived = collector()
        var handled = 0
        repeat(3) { handled += revived.runOnce() }
        assertEquals(4, handled)
        assertEquals(before + during, rows().map(::streamId))
    }

    @Test
    fun `꼴이 틀린 항목은 세고 확인해 다시 받지 않고 같은 묶음의 다른 항목은 저장된다`() {
        val good = send()
        send(VisitRecords.fields(ip = "$ADDRESS_V4:443"))
        send(VisitRecords.fields(device = "tablet"))
        send(mapOf("v" to "1", "path" to "/wiki/a"))                     // 칸 빠짐
        val good2 = send(VisitRecords.fields(ip = ADDRESS_V6))
        val reader = source()
        val collector = collector(reader)
        assertEquals(5, collector.runOnce())
        assertEquals(0L, reader.unacknowledgedCount(), "버린 항목이 미확인으로 남았다")
        assertEquals(listOf(good, good2), rows().map(::streamId))
        val counts = collector.droppedCounts()
        assertEquals(1L, counts[VisitDropReason.INVALID_IP])
        assertEquals(1L, counts[VisitDropReason.INVALID_DEVICE])
        assertEquals(1L, counts[VisitDropReason.MISSING_FIELD])
        // 다시 떠도 버린 항목을 다시 받지 않는다
        val restarted = collector(source())
        assertEquals(0, restarted.runOnce())
        assertEquals(0L, restarted.droppedCounts().values.sum())
    }

    /** 스트림 ID 를 정해 보낸다 — 표에 이미 있는 열쇠와 같은 항목을 만들려고 */
    private fun sendWithId(id: String, fields: Map<String, String>) =
        producer.opsForStream<String, String>().add(StreamRecords.mapBacked<String, String, String>(fields).withStreamKey(stream).withId(RecordId.of(id)))!!.value

    @Test
    fun `한 묶음에 이미 있는 항목과 새 항목이 섞여 오면 저장 수와 잘라 낸 수는 새로 넣은 것만 세고 나머지는 이미 있음으로 센다`() {
        // 앞 프로세스가 1000-0 · 4000-0 을 저장까지 하고 확인 전에 죽었다(같은 열쇠가 표에 있다)
        store.insertAll(listOf(VisitRecords.visit(1_000, path = "/search", screen = "search"), VisitRecords.visit(4_000)))
        sendWithId("1000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search"))   // 이미 있음 · 잘라 냄
        sendWithId("2000-0", VisitRecords.fields())                                                     // 새것
        sendWithId("3000-0", VisitRecords.fields(path = "/search?q=$SEARCH_WORDS", screen = "search"))   // 새것 · 잘라 냄
        sendWithId("4000-0", VisitRecords.fields())                                                     // 이미 있음
        val reader = source()
        val collector = collector(reader)
        assertEquals(4, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(0L, reader.unacknowledgedCount(), "이미 있는 항목을 확인하지 않아 다시 받는다")
        assertEquals(listOf("1000-0", "2000-0", "3000-0", "4000-0"), rows().map(::streamId))
        assertEquals(2L, collector.storedTotal, "이미 있던 항목까지 저장 수에 셌다")
        assertEquals(2L, collector.alreadyStored, "이미 있던 항목을 따로 세지 않았다")
        assertEquals(1L, collector.queryStripped, "이미 있던 항목의 잘라 냄까지 셌다")
        val health = VisitHealth(collector, { reader.unacknowledgedCount() }, { reader.lag() }, { store.latestStoredAt() }, { null },
            oldestStoredAt = { store.oldestStoredAt() }).report(Instant.now())
        assertEquals(2L, health["stored_since_start"], "$health")
        assertEquals(2L, health["already_stored"], "$health")
        assertEquals(1L, health["query_stripped"], "$health")
    }

    @Test
    fun `DB 가 죽은 동안은 확인하지 않고 기다리며 헬스에 드러나고 살아나면 잃지 않고 저장한다`() {
        val sink = SwitchableSink()
        val reader = source()
        val collector = collector(reader, sink)
        assertEquals(0, collector.runOnce())                              // 그룹 · 스키마 준비
        sink.databaseDown = true
        val sent = (1..5).map { send() }
        repeat(3) { collector.runOnce() }
        assertNotNull(collector.lastFailure, "DB 가 죽었는데 실패가 아니다")
        assertEquals(5L, reader.unacknowledgedCount(), "DB 가 죽은 동안 확인해 버렸다 — 잃는다")
        assertEquals(0, rows().size)
        val health = VisitHealth(collector, { reader.unacknowledgedCount() }, { reader.lag() }, { deadStore.latestStoredAt() }, { null },
            oldestStoredAt = { deadStore.oldestStoredAt() })
        val body = health.report(Instant.now())
        assertEquals("unreachable", body["status"], "$body")
        assertTrue((body["failure"] as String).startsWith("database "), "$body")
        assertEquals(5L, body["unacknowledged"])

        sink.databaseDown = false
        collector.runOnce()
        assertNull(collector.lastFailure)
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(sent, rows().map(::streamId))
    }

    @Test
    fun `저장이 실패해도 애플리케이션 로그 전체와 실패 사유와 헬스에 주소 원문이 없다`() {
        // 표에 넣을 때마다 실패하게 한다 — 드라이버의 묶음 오류 문구에는 넣은 값이 실린다
        jdbc.execute("CREATE FUNCTION $schema.refuse_visit() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RAISE EXCEPTION 'refused for test'; END \$\$")
        jdbc.execute("CREATE TRIGGER refuse_visit BEFORE INSERT ON $schema.visit_event FOR EACH ROW EXECUTE FUNCTION $schema.refuse_visit()")
        send(VisitRecords.fields(ip = ADDRESS_V4))
        send(VisitRecords.fields(ip = ADDRESS_V6, path = "/search?q=$SEARCH_WORDS"))
        val reader = source()
        val collector = collector(reader)
        CapturedLogs().use { logs ->
            repeat(2) { collector.runOnce() }
            assertNotNull(collector.lastFailure, "저장이 실패했는데 실패가 아니다")
            assertEquals(2L, reader.unacknowledgedCount())
            val health = VisitHealth(collector, { reader.unacknowledgedCount() }, { reader.lag() }, { store.latestStoredAt() }, { null },
                oldestStoredAt = { store.oldestStoredAt() }).report(Instant.now())
            assertEquals("failing", health["status"], "$health")
            val text = logs.text()
            assertTrue(text.contains("visit", ignoreCase = true), "저장 실패가 로그에 드러나지 않는다: $text")
            for (secret in listOf(ADDRESS_V4, ADDRESS_V6, VISITOR_ONE, SEARCH_WORDS)) {
                assertFalse(text.contains(secret), "로그에 $secret 이 나왔다: $text")
                assertFalse(collector.lastFailure!!.contains(secret), "실패 사유에 $secret: ${collector.lastFailure}")
                assertFalse(health.toString().contains(secret), "헬스에 $secret: $health")
            }
        }
        jdbc.execute("DROP TRIGGER refuse_visit ON $schema.visit_event")
        collector.runOnce()
        assertEquals(2, rows().size, "막힌 동안의 항목을 잃었다")
    }
}
