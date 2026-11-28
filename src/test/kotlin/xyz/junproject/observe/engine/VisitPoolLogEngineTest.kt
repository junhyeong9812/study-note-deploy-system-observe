package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.visit.CapturedLogs
import xyz.junproject.observe.visit.VisitCollector
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import xyz.junproject.observe.visit.VisitRecords.VISITOR_TWO
import xyz.junproject.observe.visit.VisitStore
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 실제 PostgreSQL + Redis — **주소가 연결 풀 층의 로그로 나가지 않는다.** 연결이 끊긴 쪽 오류(SQLSTATE `08xxx` · `57P01` 등)를 받으면
 * 연결 풀(HikariCP)이 그 연결을 버리며 드라이버의 예외를 통째로 WARN 으로 찍는다 — 그 글에는 묶음 저장의 값(주소 · 구분값)과 서버 오류의
 * 세부(DETAIL)가 실릴 수 있다. 방문의 풀 셋(수집 · 조회 · 헬스)은 **설정([ObserveConfiguration.visitStore])이 만든 그대로** 쓴다 —
 * 테스트가 따로 만든 풀이면 설정이 빠져도 통과하기 때문이다. 로그는 루트 로거 전체(풀의 로거 포함)에서 본다.
 * 표는 전용 스키마(`visit_pool_log`)에 만들고, 실패를 일으키는 트리거 · 뷰는 테스트마다 지운다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VisitPoolLogEngineTest {
    private val schema = "visit_pool_log"
    private val administration = JdbcTemplate(EngineSupport.dataSource(size = 1))
    private val connections = mutableListOf<LettuceConnectionFactory>()
    private val producer = EngineSupport.redisTemplate(connect())
    private lateinit var configuration: ObserveConfiguration
    private lateinit var store: VisitStore
    private lateinit var stream: String

    private fun connect() = EngineSupport.redisConnection().also { connections += it }

    private fun send(fields: Map<String, String>) = producer.opsForStream<String, String>().add(stream, fields)!!.value

    private fun rowCount() = administration.queryForObject("SELECT count(*) FROM $schema.visit_event", Long::class.java)!!

    /** 로그에 없어야 하는 글자 — 문서용 대역의 주소 둘과 구분값 둘 */
    private val secrets = listOf(ADDRESS_V4, ADDRESS_V6, VISITOR_ONE, VISITOR_TWO)

    @BeforeEach
    fun reset() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        configuration = ObserveConfiguration()
        // 운영과 같은 길 — 설정의 방문 풀 셋. 주소에 이미 물음표(currentSchema)가 있어도 설정이 제 값을 덧붙여야 한다
        store = configuration.visitStore(EngineSupport.databaseUrl + "?currentSchema=$schema", EngineSupport.databaseUser, EngineSupport.databasePassword)
        store.createSchema()
        stream = "observe-test-visits-pool-${UUID.randomUUID()}"
    }

    @AfterEach
    fun closePools() {
        configuration.closePools()
        producer.delete(stream)
    }

    @AfterAll
    fun close() {
        connections.forEach { it.destroy() }
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        (administration.dataSource as HikariDataSource).close()
    }

    /**
     * 표에 넣을 때마다 [failingBody] 로 실패하는 트리거를 건 채 수집을 한 바퀴 돌리고, 그동안(과 풀이 연결을 버리는 동안) 모인 로그 전체를 돌려준다.
     * 트리거는 돌아오기 전에 지운다.
     */
    private fun collectWhileInsertFails(collector: VisitCollector, failingBody: String): String {
        administration.execute("CREATE FUNCTION $schema.refuse_visit() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN $failingBody END \$\$")
        administration.execute("CREATE TRIGGER refuse_visit BEFORE INSERT ON $schema.visit_event FOR EACH ROW EXECUTE FUNCTION $schema.refuse_visit()")
        try {
            return CapturedLogs().use { logs ->
                collector.runOnce()
                Thread.sleep(1_000)                                   // 풀이 깨진 연결을 닫으며 남기는 줄까지
                logs.text()
            }
        } finally {
            administration.execute("DROP TRIGGER IF EXISTS refuse_visit ON $schema.visit_event")
            administration.execute("DROP FUNCTION IF EXISTS $schema.refuse_visit()")
        }
    }

    private fun collectorWithTwoVisits(): VisitCollector {
        send(VisitRecords.fields(ip = ADDRESS_V4, visitor = VISITOR_ONE))
        send(VisitRecords.fields(ip = ADDRESS_V6, visitor = VISITOR_TWO, device = "mobile"))
        return VisitCollector(RedisMetricStreamSource(EngineSupport.redisTemplate(connect()), stream, "observe"), store, block = Duration.ofMillis(200))
    }

    private fun assertNoSecrets(text: String) {
        for (secret in secrets) {
            assertFalse(text.contains(secret), "로그에 $secret 이 나왔다:\n" + text.lines().filter { it.contains(secret) }.take(5).joinToString("\n"))
        }
    }

    @Test
    fun `저장이 연결 끊김 오류(08006)로 실패해도 풀의 로그를 포함한 로그 전체에 주소와 구분값이 없다`() {
        val collector = collectorWithTwoVisits()
        val text = collectWhileInsertFails(collector,
            "RAISE EXCEPTION 'connection lost (test)' USING ERRCODE = '08006', DETAIL = 'row ' || NEW.ip || ' ' || NEW.visitor;")
        assertNotNull(collector.lastFailure, "저장이 실패하지 않았다 — 검사가 헛돈다")
        assertTrue(text.contains("marked as broken"), "풀이 연결을 버리는 줄이 로그에 없다 — 풀의 로그를 모으지 못해 검사가 헛돈다:\n$text")
        assertNoSecrets(text)
        assertEquals(0L, rowCount())
        collector.runOnce()
        assertEquals(2L, rowCount(), "막힌 동안의 항목을 잃었다")
    }

    @Test
    fun `저장 중에 연결이 실제로 끊겨도(pg_terminate_backend) 풀의 로그를 포함한 로그 전체에 주소와 구분값이 없다`() {
        val collector = collectorWithTwoVisits()
        val text = collectWhileInsertFails(collector, "PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NEW;")
        assertNotNull(collector.lastFailure, "저장이 실패하지 않았다 — 검사가 헛돈다")
        assertTrue(text.contains("marked as broken"), "풀이 연결을 버리는 줄이 로그에 없다 — 풀의 로그를 모으지 못해 검사가 헛돈다:\n$text")
        assertNoSecrets(text)
        assertEquals(0L, rowCount())
        collector.runOnce()
        assertEquals(2L, rowCount(), "막힌 동안의 항목을 잃었다")
    }

    @Test
    fun `조회와 헬스 풀이 연결 끊김 오류를 받아도 서버 오류의 세부에 실린 주소와 구분값이 로그 전체에 없다`() {
        val now = Instant.now()
        store.insertAll(listOf(VisitRecords.visit(now.minus(Duration.ofHours(1)).toEpochMilli(), ip = ADDRESS_V4, visitor = VISITOR_ONE)))
        // 표를 줄마다 오류를 내는 뷰 뒤로 숨긴다 — 오류의 세부(DETAIL)에 그 줄의 주소 · 구분값을 싣는다
        administration.execute("ALTER TABLE $schema.visit_event RENAME TO visit_event_stored")
        administration.execute("""CREATE FUNCTION $schema.refuse_read(address text, visitor text) RETURNS boolean LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN RAISE EXCEPTION 'connection lost (test)' USING ERRCODE = '08006', DETAIL = 'row ' || address || ' ' || visitor; END ${'$'}${'$'}""")
        administration.execute("CREATE VIEW $schema.visit_event AS SELECT * FROM $schema.visit_event_stored WHERE $schema.refuse_read(ip, visitor)")
        val text = try {
            CapturedLogs().use { logs ->
                assertThrows(DataAccessException::class.java) { store.summary(7, now) }        // 조회 풀
                assertThrows(DataAccessException::class.java) { store.latestStoredAt() }       // 헬스 풀
                Thread.sleep(1_000)
                logs.text()
            }
        } finally {
            administration.execute("DROP VIEW IF EXISTS $schema.visit_event")
            administration.execute("DROP FUNCTION IF EXISTS $schema.refuse_read(text, text)")
            administration.execute("ALTER TABLE $schema.visit_event_stored RENAME TO visit_event")
        }
        assertTrue(Regex("marked as broken").findAll(text).count() >= 2,
            "조회 · 헬스 풀이 연결을 버리는 줄이 둘 다 로그에 있지 않다 — 검사가 헛돈다:\n$text")
        assertNoSecrets(text)
    }
}
