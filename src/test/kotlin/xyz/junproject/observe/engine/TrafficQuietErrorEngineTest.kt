package xyz.junproject.observe.engine

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.traffic.TrafficRecords
import xyz.junproject.observe.traffic.TrafficStore
import java.time.Instant

/**
 * 실제 PostgreSQL 의 실제 오류 — 통신 저장의 연결 풀(앱이 만드는 그대로 — `ObserveConfiguration.trafficStore`)은 드라이버 예외 글에
 * 서버 오류의 상세(DETAIL — 행의 값)와 묶음 문장의 값을 싣지 않는다(`logServerErrorDetail=false`). 항목의 경로가 예외 글을 타고 로그에 새지 않게.
 * 대조로, 그 설정이 없는 풀에서는 같은 오류의 글에 값이 실리는 것을 먼저 본다(이 테스트가 새는 것을 알아볼 수 있는지).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TrafficQuietErrorEngineTest {
    private val schema = "traffic_quiet"
    private val administrationSource = EngineSupport.dataSource(size = 2)
    private val administration = JdbcTemplate(administrationSource)
    private val configuration = ObserveConfiguration()
    private val now = Instant.parse("2026-11-25T04:00:00Z")
    private val marker = "/MARKER-secret-path"

    @BeforeEach
    fun reset() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
    }

    @AfterAll
    fun close() {
        configuration.closePools()
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationSource.close()
    }

    /** 요청별 줄을 넣을 때 그 줄의 경로를 오류의 상세에 실어 거절하는 트리거 */
    private fun refuseWithDetail(store: TrafficStore) {
        store.createSchema()
        administration.execute("""
            CREATE FUNCTION $schema.observe_test_detail() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN RAISE EXCEPTION 'refused by test' USING DETAIL = 'path ' || NEW.path; END ${'$'}${'$'}""")
        administration.execute("CREATE TRIGGER observe_test_detail BEFORE INSERT ON $schema.traffic_request FOR EACH ROW EXECUTE FUNCTION $schema.observe_test_detail()")
    }

    private fun failureText(store: TrafficStore): String {
        val entry = TrafficRecords.entry(1, now.minusSeconds(1), now,
            TrafficRecords.body(listOf(TrafficRecords.segment(now.minusSeconds(30), listOf(200))), listOf(TrafficRecords.request(now.minusSeconds(5), path = marker))))
        val error = assertThrows(Exception::class.java) { store.store(entry) }
        val text = generateSequence<Throwable>(error) { it.cause }.joinToString("\n") { "${it.javaClass.name}: ${it.message}" } +
            generateSequence<Throwable>(error) { it.cause }.filterIsInstance<java.sql.SQLException>().flatMap { generateSequence(it) { next -> next.nextException } }
                .joinToString("\n") { it.message ?: "" }
        assertTrue(text.contains("refused by test"), "트리거의 오류가 아닌 다른 이유로 실패했다: $text")
        return text
    }

    @Test
    fun `대조 — 설정이 없는 풀이면 오류 글에 경로가 실린다`() {
        val loud = com.zaxxer.hikari.HikariDataSource(com.zaxxer.hikari.HikariConfig().apply {
            jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@TrafficQuietErrorEngineTest.schema}"; username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 2
        })
        try {
            val store = JdbcTemplate(loud).let { TrafficStore(it, it, it) }
            refuseWithDetail(store)
            assertTrue(failureText(store).contains("MARKER"), "대조가 서지 않는다 — 이 오류로는 새는 것을 알아볼 수 없다")
        } finally {
            loud.close()
        }
    }

    @Test
    fun `통신 저장의 연결 풀은 실제 오류에서도 예외 글에 항목의 값을 싣지 않는다`() {
        val store = configuration.trafficStore(EngineSupport.databaseUrl + "?currentSchema=$schema", EngineSupport.databaseUser, EngineSupport.databasePassword, "10m")
        refuseWithDetail(store)
        val text = failureText(store)
        assertFalse(text.contains("MARKER"), "드라이버 예외 글에 항목의 경로가 실렸다: $text")
    }
}
