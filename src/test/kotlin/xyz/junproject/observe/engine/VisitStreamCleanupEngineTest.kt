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
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.visit.CapturedLogs
import xyz.junproject.observe.visit.RedisVisitStreamCleaner
import xyz.junproject.observe.visit.Visit
import xyz.junproject.observe.visit.VisitCollector
import xyz.junproject.observe.visit.VisitDropReason
import xyz.junproject.observe.visit.VisitInsertion
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitSchedule
import xyz.junproject.observe.visit.VisitSink
import xyz.junproject.observe.visit.VisitStore
import xyz.junproject.observe.visit.VisitStreamCleaner
import xyz.junproject.observe.visit.VisitStreamTrimResult
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

/**
 * 실제 Redis 7.4 + PostgreSQL — V2b: 방문 항목은 저장 → 확인 → **지움**(`XDEL`)의 차례로 스트림에서 사라진다. 저장에 실패한 항목은 남는다 ·
 * 지우기의 실패는 다음 바퀴에 다시 지운다 · 두 자리(같은 소비자 그룹 · 같은 소비자 이름)가 함께 돌아도 저장되지 않은 항목은 지워지지 않는다 ·
 * 하루 한 번의 정리가 `now − 90일` 보다 오래된 항목을 잘라 낸다(`XTRIM MINID` — 같은 밀리초는 남는다) · 로그에 주소가 없다.
 * 불변식: **스트림에서 사라진 방문 항목은 모두 표에 있다**(꼴이 틀려 버린 항목과 90일 넘은 항목을 빼고).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VisitStreamCleanupEngineTest {
    private val schema = "visit_stream_cleanup"
    private val administrationSource = EngineSupport.dataSource(size = 1)
    private val administration = JdbcTemplate(administrationSource)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@VisitStreamCleanupEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = VisitStore(jdbc, jdbc, jdbc)
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producerTemplate = EngineSupport.redisTemplate(connect())
    private val producer = producerTemplate.opsForStream<String, String>()
    private lateinit var stream: String
    private val group = "observe"
    private val logs = CapturedLogs()

    private fun connect() = EngineSupport.redisConnection().also { connections += it }
    private fun source(connection: LettuceConnectionFactory = connect()) = RedisMetricStreamSource(EngineSupport.redisTemplate(connection), stream, group)
    private fun cleaner(connection: LettuceConnectionFactory = connect()) = RedisVisitStreamCleaner(EngineSupport.redisTemplate(connection), stream)
    private fun collector(source: RedisMetricStreamSource = source(), sink: VisitSink = store, cleaner: VisitStreamCleaner = cleaner(), batchSize: Int = 200) =
        VisitCollector(source, sink, batchSize = batchSize, block = Duration.ofMillis(200), cleaner = cleaner)

    private fun send(fields: Map<String, String> = VisitRecords.fields()) = producer.add(stream, fields)!!.value
    private fun sendAt(id: String) = producer.add(StreamRecords.newRecord().`in`(stream).withId(RecordId.of(id)).ofMap(VisitRecords.fields()))!!.value
    private fun streamIds(): List<String> = producer.range(stream, Range.unbounded<String>())!!.map { it.id.value }
    private fun storedIds(): Set<String> = jdbc.queryForList("SELECT stream_ms || '-' || stream_seq FROM visit_event", String::class.java).filterNotNull().toSet()

    @BeforeEach
    fun reset() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        store.createSchema()
        stream = "observe-test-visits-${UUID.randomUUID()}"
    }

    @AfterEach fun dropStream() { producerTemplate.delete(stream) }

    @AfterAll
    fun close() {
        logs.close()
        connections.forEach { it.destroy() }
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administrationSource.close()
    }

    /** 저장을 켰다 껐다 하는 길 — [failFor] 에 든 ID 가 묶음에 있으면 그 묶음 전체가 실패한다(드라이버의 묶음 저장과 같다) */
    private inner class SwitchableSink : VisitSink {
        @Volatile var failing = false
        @Volatile var failFor: Set<String> = emptySet()
        override fun createSchema() = store.createSchema()
        override fun insertAll(visits: List<Visit>): VisitInsertion {
            if (failing || visits.any { it.id.toString() in failFor }) throw IllegalStateException("저장 실패(테스트) $ADDRESS_V4")
            return store.insertAll(visits)
        }
    }

    /** 지우기를 켰다 껐다 하는 진짜 지우개 */
    private inner class SwitchableCleaner(private val delegate: VisitStreamCleaner = cleaner()) : VisitStreamCleaner by delegate {
        @Volatile var failing = false
        override fun delete(ids: List<String>): Long { if (failing) throw IllegalStateException("READONLY $ADDRESS_V4"); return delegate.delete(ids) }
        override fun trimOlderThan(cutoff: Instant): Long { if (failing) throw IllegalStateException("READONLY $ADDRESS_V4"); return delegate.trimOlderThan(cutoff) }
    }

    @Test
    fun `저장하고 확인한 방문 항목과 꼴이 틀려 버린 항목은 스트림에서 지워진다`() {
        val good = List(3) { send(VisitRecords.fields(ip = if (it == 0) ADDRESS_V6 else ADDRESS_V4)) }
        val broken = send(VisitRecords.fields(ip = "$ADDRESS_V4:443"))
        val reader = source()
        val collector = collector(reader)
        assertEquals(4, collector.runOnce())
        assertEquals(good.toSet(), storedIds())
        assertEquals(emptyList<String>(), streamIds(), "저장 · 확인한 항목이 스트림에 남았다 — 주소가 든 원문이 Redis 에 남는다")
        assertEquals(0L, producer.size(stream))
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(4L, collector.streamDeleted)
        assertEquals(1L, collector.droppedCounts()[VisitDropReason.INVALID_IP])
        assertFalse(broken in streamIds())
    }

    @Test
    fun `저장에 실패한 항목은 스트림에 남고 미확인으로 남으며 저장된 뒤에야 지워진다`() {
        val ids = List(3) { send() }
        val sink = SwitchableSink().apply { failing = true }
        val reader = source()
        val collector = collector(reader, sink)
        assertEquals(0, collector.runOnce())
        assertEquals(ids, streamIds(), "저장에 실패한 항목을 지웠다")
        assertEquals(3L, reader.unacknowledgedCount())
        sink.failing = false
        collector.runOnce()
        assertEquals(ids.toSet(), storedIds())
        assertEquals(emptyList<String>(), streamIds())
    }

    @Test
    fun `묶음 둘 중 뒤의 묶음만 저장에 실패하면 실패한 묶음의 항목만 스트림에 남는다`() {
        val ids = List(4) { send() }
        val sink = SwitchableSink().apply { failFor = setOf(ids[3]) }
        val collector = collector(sink = sink, batchSize = 2)
        repeat(2) { collector.runOnce() }
        assertEquals(ids.take(2).toSet(), storedIds())
        assertEquals(ids.drop(2), streamIds(), "실패한 묶음만 남아야 한다")
        assertEquals(ids.toSet(), storedIds() + streamIds(), "저장되지도 남지도 않은 항목이 있다")
    }

    @Test
    fun `지우기가 실패해도 수집은 실패가 아니고 다음 바퀴에 다시 지우며 다시 저장하지 않는다`() {
        val ids = List(2) { send() }
        val switchable = SwitchableCleaner().apply { failing = true }
        val reader = source()
        val collector = collector(reader, cleaner = switchable)
        assertEquals(2, collector.runOnce())
        assertNull(collector.lastFailure)
        assertEquals(ids.toSet(), storedIds())
        assertEquals(ids, streamIds(), "실패했다는데 지워졌다")
        assertEquals(0L, reader.unacknowledgedCount(), "지우기의 실패로 확인을 하지 않았다 — 다시 받아 다시 저장하게 된다")
        assertEquals(2, collector.streamDeletePending)
        switchable.failing = false
        collector.runOnce()
        assertEquals(emptyList<String>(), streamIds())
        assertEquals(2L, collector.storedTotal)
        assertEquals(0L, collector.alreadyStored, "다시 받아 저장을 시도했다")
        assertFalse(logs.text().contains(ADDRESS_V4), "지우기 실패의 로그에 주소가 실렸다")
    }

    @Test
    fun `한 자리가 읽어 가 저장하지 않은 항목은 다른 자리가 저장한 뒤에만 지우고 다른 자리의 저장이 실패하면 남는다`() {
        val ids = List(3) { send() }
        val blueSource = source()
        blueSource.ensureGroup()
        assertEquals(3, blueSource.readNew(100, Duration.ofMillis(200)).size)            // 파랑이 읽어 가고 저장 전에 멈췄다
        val greenSink = SwitchableSink().apply { failing = true }
        val green = collector(sink = greenSink)
        green.runOnce()                                                                   // 초록이 미확인분을 다시 받지만 저장에 실패
        assertEquals(ids, streamIds(), "저장되지 않은 다른 자리의 미확인분을 지웠다")
        greenSink.failing = false
        green.runOnce()
        assertEquals(ids.toSet(), storedIds(), "다른 자리가 남긴 미확인분을 거둬 저장하지 않았다")
        assertEquals(emptyList<String>(), streamIds(), "거둬 저장한 뒤에 지우지 않았다")
        // 파랑이 뒤늦게 같은 항목을 지워도 오류가 아니다
        assertEquals(0L, cleaner().delete(ids))
    }

    @Test
    fun `두 자리가 함께 돌아도 스트림에서 사라진 항목은 모두 표에 있고 끝나면 스트림이 빈다`() {
        val ids = List(300) { send(VisitRecords.fields(path = "/wiki/item-$it")) }
        val failures = ConcurrentLinkedQueue<Throwable>()
        val start = CountDownLatch(1)
        val places = List(2) { index ->
            val collector = collector(batchSize = 7)
            thread(name = "observe-test-place-$index") {
                start.await()
                repeat(80) { runCatching { collector.runOnce() }.onFailure { failures += it }; if (collector.lastFailure != null) failures += IllegalStateException(collector.lastFailure) }
            }
        }
        val checks = ConcurrentLinkedQueue<String>()
        val watcher = thread(name = "observe-test-watcher") {
            start.await()
            repeat(40) {
                val present = streamIds().toSet()
                val stored = storedIds()
                val missing = ids.filter { it !in present && it !in stored }
                if (missing.isNotEmpty()) checks += "저장되지 않았는데 지워짐: ${missing.take(3)}"
                Thread.sleep(25)
            }
        }
        start.countDown()
        places.forEach { it.join(60_000) }
        watcher.join(10_000)
        assertEquals(emptyList<Throwable>(), failures.toList())
        assertEquals(emptyList<String>(), checks.toList())
        assertEquals(ids.toSet(), storedIds())
        assertEquals(emptyList<String>(), streamIds())
    }

    @Test
    fun `하루 한 번의 정리는 now − 90일 보다 앞선 밀리초의 항목만 잘라 내고 같은 밀리초 · 뒤의 항목은 남긴다`() {
        val now = Instant.parse("2026-11-28T03:40:00Z")
        val cutoff = now.minus(Duration.ofDays(90)).toEpochMilli()
        val older = sendAt("${cutoff - 1}-0")
        val oldest = listOf(older)
        val edge = sendAt("$cutoff-0")
        val newer = sendAt("${cutoff + 1}-0")
        val recent = sendAt("${now.toEpochMilli()}-0")
        val schedule = VisitSchedule(collector(), store, cleaner = cleaner())
        schedule.deleteOldVisits(now)
        assertEquals(listOf(edge, newer, recent), streamIds(), "경계가 DB 삭제(stream_ms < now − 90일)와 다르다 — 잘라 낸 것: $oldest")
        assertEquals(VisitStreamTrimResult(now, 1, null), schedule.lastStreamTrim)
    }

    @Test
    fun `다시 지울 목록의 상한을 넘어 잊은 항목은 스트림에 남아 있다가 하루 정리가 자른다`() {
        val ids = List(3) { send() }
        val switchable = SwitchableCleaner().apply { failing = true }
        val reader = source()
        val collector = VisitCollector(reader, store, batchSize = 200, block = Duration.ofMillis(200), cleaner = switchable, deletePendingLimit = 1)
        collector.runOnce()
        assertEquals(ids.toSet(), storedIds())
        assertEquals(2L, collector.streamDeleteForgotten)
        switchable.failing = false
        collector.runOnce()                                                                       // 기억한 하나(가장 새 것)만 다시 지운다
        assertEquals(ids.take(2), streamIds(), "잊은 두 항목(가장 오래된 것)이 스트림에 남아 있어야 한다 — 안전망이 거둘 몫")
        val schedule = VisitSchedule(collector, store)
        assertEquals(2L, schedule.trimOldVisitStream(Instant.now().plus(Duration.ofDays(91))), "안전망이 잊은 항목을 자르지 않았다")
        assertEquals(emptyList<String>(), streamIds())
        assertEquals(ids.toSet(), storedIds(), "자른 것은 스트림의 사본뿐 — 표의 줄은 그대로")
        assertFalse(logs.text().contains(ADDRESS_V4))
    }

    @Test
    fun `90일 넘게 저장되지 못한 미확인 항목도 정리가 잘라 내고 수집은 그것을 trimmed 로 세어 확인한다`() {
        val now = Instant.parse("2026-11-28T03:40:00Z")
        val cutoff = now.minus(Duration.ofDays(90)).toEpochMilli()
        sendAt("${cutoff - 10}-0")
        val kept = sendAt("${now.toEpochMilli() - 1_000}-0")
        val reader = source()
        reader.ensureGroup()
        assertEquals(2, reader.readNew(100, Duration.ofMillis(200)).size)               // 둘 다 미확인 — 저장하지 못한 채 90일이 지났다
        VisitSchedule(collector(), store, cleaner = cleaner()).deleteOldVisits(now)
        assertEquals(listOf(kept), streamIds(), "90일 넘은 미확인 항목을 남겼다 — 주소가 든 원문이 90일을 넘는다")
        val collector = collector(reader)
        collector.runOnce()
        assertEquals(1L, collector.droppedCounts()[VisitDropReason.TRIMMED])
        assertEquals(setOf(kept), storedIds())
        assertEquals(0L, reader.unacknowledgedCount())
        assertEquals(emptyList<String>(), streamIds())
    }

    @Test
    fun `정리가 실패하면 결과에 종류 이름만 남고 DB 삭제는 돈다 · 로그에 주소가 없다`() {
        val now = Instant.parse("2026-11-28T03:40:00Z")
        sendAt("${now.minus(Duration.ofDays(91)).toEpochMilli()}-0")
        val switchable = SwitchableCleaner().apply { failing = true }
        val schedule = VisitSchedule(collector(), store, cleaner = switchable)
        schedule.deleteOldVisits(now)
        assertEquals(VisitStreamTrimResult(now, null, "IllegalStateException"), schedule.lastStreamTrim)
        assertEquals(0, schedule.lastRetention?.deleted, "정리의 실패로 DB 삭제가 돌지 않았다")
        assertEquals(1L, producer.size(stream))
        assertFalse(logs.text().contains(ADDRESS_V4))
    }
}
