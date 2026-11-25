package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 PostgreSQL — 명세 §16.3 (재점검): 발번기의 캐시가 1 이 아니면 그 구간은 실패 · 발번기 값을 못 읽으면 실패 · `firing` 은 표를 만들지 않는다.
 * 사례마다 전용 스키마를 쓰고(시퀀스 설정 · 역할이 다른 클래스로 새지 않게) 끝에 스키마 · 역할 · 풀을 지운다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReviewFourEngineTest {
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()
    private val testSchemas = mutableListOf<String>()
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val sequence = AtomicInteger()

    private fun freshSchema(name: String): String {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $name CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $name")
        testSchemas += name
        return name
    }

    private fun pool(schema: String, size: Int, user: String = EngineSupport.databaseUser, password: String = EngineSupport.databasePassword) =
        HikariDataSource(HikariConfig().apply {
            jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=$schema"
            username = user; this.password = password; maximumPoolSize = size
        }).also { testPools += it }

    @AfterEach
    fun cleanUp() {
        testPools.forEach { it.close() }; testPools.clear()
        testSchemas.forEach { administrationJdbc.execute("DROP SCHEMA IF EXISTS $it CASCADE") }; testSchemas.clear()
    }

    @AfterAll fun close() = administration.close()

    private val probe = ContainerValues("probe-app", "0000000000aa", "study-note-deploy-system-probe", null, null, null, "running")

    private fun sample(measuredAt: Instant, receivedAt: Instant, cpu: Double) =
        ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), "ci-agent-158", measuredAt, receivedAt, 30_000,
            HostValues(cpuPct = cpu), listOf(probe), emptyList(), emptyList())

    private fun rawAt(store: MetricStore, at: Instant) = store.containers("ci-agent-158", at, at).isNotEmpty()

    private fun storeOn(schema: String, size: Int = 1) = JdbcTemplate(pool(schema, size)).let { MetricStore(it, it, it) }

    private fun sequenceOf(schema: String) =
        administrationJdbc.queryForObject("SELECT pg_get_serial_sequence('$schema.metric_sample', 'insert_order')", String::class.java)
            ?: fail("metric_sample.insert_order 의 발번기를 찾지 못했다")

    // ── 1 발번기의 캐시 ──────────────────────────────────

    @Test
    fun `발번기의 캐시가 1 이 아니면 그 구간은 실패로 세고 평균도 계산함 표시도 없어 원본이 지워지지 않는다`() {
        val schema = freshSchema("sequence_cache")
        val store = storeOn(schema)
        store.createSchema()
        administrationJdbc.execute("ALTER SEQUENCE ${sequenceOf(schema)} CACHE 10")
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
        val outcome = store.rollUpBuckets(now)
        assertTrue(outcome.failed >= 1, "캐시 10 인 발번기로 계산했다: $outcome")
        assertNotNull(outcome.firstFailure, "실패의 종류가 비었다")
        assertEquals(emptyList<Any>(), store.rollups("ci-agent-158", MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5))))
        store.deleteExpired(now.plusSeconds(1))
        assertTrue(rawAt(store, bucket.plusSeconds(10)), "계산하지 못한 구간의 원본을 지웠다")
    }

    @Test
    fun `캐시 10 인 발번기에서 평균 뒤에 같은 연결로 저장된 원본을 잃지 않는다`() {
        val schema = freshSchema("sequence_cache_loss")
        val store = storeOn(schema)                                                    // 연결 하나 — 같은 연결이 캐시한 번호를 이어 쓴다
        store.createSchema()
        administrationJdbc.execute("ALTER SEQUENCE ${sequenceOf(schema)} CACHE 10")
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))        // 번호 1(발번기는 10 까지 내줬다고 적는다)
        store.rollUpBuckets(now)
        store.insertAll(listOf(sample(bucket.plusSeconds(40), now.minusSeconds(10), 30.0)))           // 번호 2 — 수신은 계산 시각보다 앞
        store.rollUpBuckets(now.plusSeconds(1))
        store.deleteExpired(now.plusSeconds(2))
        val samples = store.rollups("ci-agent-158", MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5))).singleOrNull()?.values?.get("cpu_pct")?.samples
        assertTrue(samples == 2L || rawAt(store, bucket.plusSeconds(40)),
            "평균에 들지 못한 원본이 지워졌다(평균의 표본 $samples) — 캐시한 번호를 '이미 본 것'으로 쳤다")
    }

    // ── 2 발번기 값을 못 읽음 ────────────────────────────

    @Test
    fun `발번기 값을 읽을 권한이 없으면 그 구간은 실패로 세고 원본이 지워지지 않는다`() {
        val schema = freshSchema("sequence_denied")
        val role = "observe_test_no_sequence"
        administrationJdbc.execute("DROP ROLE IF EXISTS $role")
        administrationJdbc.execute("CREATE ROLE $role LOGIN PASSWORD 'no-sequence-test'")
        try {
            administrationJdbc.execute("ALTER SCHEMA $schema OWNER TO $role")           // 표는 그 역할이 만들고 가진다(본체의 표 만들기 그대로)
            val limited = JdbcTemplate(pool(schema, 2, role, "no-sequence-test")).let { MetricStore(it, it, it) }
            limited.createSchema()
            val bucket = now.minus(Duration.ofDays(8))
            limited.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
            // 그 뒤 발번기의 권한만 거둔다 — 원본이 있는데 발번기 값을 읽지 못하는 상황
            administrationJdbc.execute("REVOKE ALL ON SEQUENCE ${sequenceOf(schema)} FROM $role")
            val outcome = limited.rollUpBuckets(now)
            assertTrue(outcome.failed >= 1, "발번기 값을 못 읽었는데 0 으로 넘겨 성공으로 셌다: $outcome")
            assertNotNull(outcome.firstFailure)
            limited.deleteExpired(now.plusSeconds(1))
            assertTrue(rawAt(limited, bucket.plusSeconds(10)), "계산하지 못한 구간의 원본을 지웠다")
        } finally {
            testPools.forEach { it.close() }; testPools.clear()
            administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
            administrationJdbc.execute("DROP OWNED BY $role")
            administrationJdbc.execute("DROP ROLE IF EXISTS $role")
        }
    }

    @Test
    fun `발번기 값이 비어 오면(아직 내준 번호가 없다고 답함) 그 구간은 실패로 세고 원본이 지워지지 않는다`() {
        val schema = freshSchema("sequence_empty")
        val store = storeOn(schema, size = 2)
        store.createSchema()
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), bucket.plusSeconds(11), 10.0)))
        // 원본은 있는데 발번기는 "아직 내준 번호 없음"(pg_sequence_last_value 가 null) — 칸과 발번기를 끊는 길(OWNED BY NONE)은
        // 이 칸이 IDENTITY 라 막혀 있어, 같은 "값이 비어 옴"을 RESTART 로 만든다
        administrationJdbc.execute("ALTER SEQUENCE ${sequenceOf(schema)} RESTART")
        assertNull(administrationJdbc.queryForObject("SELECT pg_sequence_last_value('${sequenceOf(schema)}'::regclass)", Long::class.java),
            "시험이 성립하지 않는다 — 발번기 값이 비지 않았다")
        val outcome = store.rollUpBuckets(now)
        assertTrue(outcome.failed >= 1, "발번기 값이 비었는데 0 으로 넘겨 성공으로 셌다: $outcome")
        assertNotNull(outcome.firstFailure)
        store.deleteExpired(now.plusSeconds(1))
        assertTrue(rawAt(store, bucket.plusSeconds(10)), "계산하지 못한 구간의 원본을 지웠다")
    }

    // ── 3 firing 은 표를 만들지 않는다 ───────────────────

    private fun thresholdTables(schema: String) = administrationJdbc.queryForObject(
        "SELECT count(*) FROM pg_tables WHERE schemaname = ? AND tablename LIKE 'resource_%'", Long::class.java, schema)

    @Test
    fun `판정 표가 없으면 firing 은 빈 목록이고 표를 만들지 않으며 latest 도 firing 빈 배열로 답한다`() {
        val schema = freshSchema("firing_without_tables")
        val metrics = storeOn(schema, size = 2).also { it.createSchema() }
        val thresholds = ThresholdStore(JdbcTemplate(pool(schema, 2)))
        assertEquals(emptyList<Any>(), thresholds.firing())
        assertEquals(0L, thresholdTables(schema), "어드민 조회의 길이 판정 표를 만들었다")
        val response = MetricController(metrics, "secret", "", thresholds).latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }, now)
        assertEquals(200, response.statusCode.value(), "${response.body}")
        @Suppress("UNCHECKED_CAST")
        assertEquals(emptyList<Any>(), ((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["firing"])
        assertEquals(0L, thresholdTables(schema))
    }
}
