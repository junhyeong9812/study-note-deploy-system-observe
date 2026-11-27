package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.visit.VisitCollector
import xyz.junproject.observe.visit.VisitHealth
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 실제 PostgreSQL + Redis — **저장한 수가 드라이버 설정에 흔들리지 않는다.** 운영 주소에 `reWriteBatchedInserts=true` 가 들어 있어도
 * 방문 풀 셋은 설정([ObserveConfiguration.visitStore])이 그 값을 false 로 박아 묶음의 줄마다 결과를 받는다 — 켜진 채면 드라이버가 모든 줄에
 * "결과 모름"을 돌려줘 다시 받은 항목도 새로 넣은 것으로 세어진다. 표는 전용 스키마(`visit_batch_rewrite`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VisitBatchRewriteEngineTest {
    private val schema = "visit_batch_rewrite"
    private val administration = JdbcTemplate(EngineSupport.dataSource(size = 1))
    private val redisConnection = EngineSupport.redisConnection()
    private val producer = EngineSupport.redisTemplate(redisConnection)
    private val stream = "observe-test-visits-rewrite-${UUID.randomUUID()}"

    @AfterAll
    fun close() {
        producer.delete(stream)
        redisConnection.destroy()
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        (administration.dataSource as HikariDataSource).close()
    }

    private fun sendWithId(id: String, fields: Map<String, String>) =
        producer.opsForStream<String, String>().add(StreamRecords.mapBacked<String, String, String>(fields).withStreamKey(stream).withId(RecordId.of(id)))!!.value

    @Test
    fun `주소에 reWriteBatchedInserts=true 를 줘도 이미 있는 둘과 새것 둘의 묶음은 저장 2 이미 있음 2 로 센다`() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        val configuration = ObserveConfiguration()
        try {
            val store = configuration.visitStore(EngineSupport.databaseUrl + "?currentSchema=$schema&reWriteBatchedInserts=true",
                EngineSupport.databaseUser, EngineSupport.databasePassword)
            store.createSchema()
            // 앞 프로세스가 1000-0 · 3000-0 을 저장까지 하고 확인 전에 죽었다(같은 열쇠가 표에 있다)
            for (millis in listOf(1_000L, 3_000L)) {
                administration.update("""INSERT INTO $schema.visit_event (stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip)
                    VALUES (?, 0, '/wiki/kubernetes/pod', 'topic', NULL, 'desktop', ?, ?)""", millis, VISITOR_ONE, ADDRESS_V4)
            }
            for (id in listOf("1000-0", "2000-0", "3000-0", "4000-0")) sendWithId(id, VisitRecords.fields())
            val reader = RedisMetricStreamSource(producer, stream, "observe")
            val collector = VisitCollector(reader, store, block = Duration.ofMillis(200))
            assertEquals(4, collector.runOnce())
            assertNull(collector.lastFailure)
            assertEquals(0L, reader.unacknowledgedCount())
            assertEquals(4L, administration.queryForObject("SELECT count(*) FROM $schema.visit_event", Long::class.java))
            assertEquals(2L, collector.storedTotal, "이미 있던 항목까지 저장 수에 셌다 — 묶음의 결과를 줄마다 받지 못했다")
            assertEquals(2L, collector.alreadyStored, "이미 있던 항목을 이미 있음으로 세지 못했다")
            assertEquals(0L, collector.storeResultUnknown, "드라이버가 결과 모름을 돌려줬다 — 주소의 reWriteBatchedInserts 가 false 로 박히지 않았다")
            val health = VisitHealth(collector, { reader.unacknowledgedCount() }, { reader.lag() }, { store.latestStoredAt() }, { null },
                oldestStoredAt = { store.oldestStoredAt() }).report(Instant.now())
            assertEquals(2L, health["stored_since_start"], "$health")
            assertEquals(2L, health["already_stored"], "$health")
            assertEquals(0L, health["store_result_unknown"], "$health")
        } finally {
            configuration.closePools()
        }
    }
}
