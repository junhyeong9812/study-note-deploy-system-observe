package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricAggregate
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricDropReason
import xyz.junproject.observe.metric.MetricRecords
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 PostgreSQL — 명세 §15 · §15.1 (리뷰 루프 1) 의 저장 · 조회: 서버의 마지막 수신 시각은 원본과 따로(삭제 뒤에도 · 뒤로 가지 않음 · 저장과 같은 트랜잭션),
 * `latest` 의 `age_ms` · `stale` 은 그 값으로, 6일 넘게 늦은 항목은 버려 이미 지워진 구간의 평균을 덮지 않음, 5분 평균에서 읽을 때 겹치는 구간은 통째로 한 점,
 * 컨테이너 · GPU 원본의 측정 시각 인덱스. 표는 전용 스키마(`metric_review`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricReviewEngineTest {
    private val schema = "metric_review"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@MetricReviewEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val store = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private fun sample(measuredAt: Instant, receivedAt: Instant = measuredAt.plusSeconds(1), server: String = "ci-agent-158", cpu: Double = 10.0,
                       containers: List<ContainerValues> = emptyList()) =
        ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), server, measuredAt, receivedAt, 30_000,
            HostValues(cpuPct = cpu, load1 = 0.1), containers, emptyList(), emptyList())

    // ── 마지막 수신 시각 ─────────────────────────────────

    @Test
    fun `원본이 7일 뒤 지워져도 그 서버의 마지막 수신 시각은 남는다`() {
        val old = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(old, receivedAt = old.plusSeconds(1), server = "ci-agent-9")))
        store.rollUp(now)
        assertEquals(1, store.deleteExpired(now).rawDeleted, "원본이 지워져야 이 사례가 성립한다")
        assertEquals(mapOf("ci-agent-9" to old.plusSeconds(1)), store.lastReceivedOfEveryServer())
    }

    @Test
    fun `수신 시각이 더 이른 묶음을 나중에 저장해도 마지막 수신 시각은 줄지 않는다`() {
        store.insertAll(listOf(sample(now.minusSeconds(30), receivedAt = now)))
        store.insertAll(listOf(sample(now.minusSeconds(60), receivedAt = now.minusSeconds(40))))       // 다른 자리가 늦게 커밋했다
        assertEquals(now, store.lastReceivedOfEveryServer()["ci-agent-158"])
    }

    @Test
    fun `저장이 되돌려지면 마지막 수신 시각도 그대로다`() {
        val broken = ContainerValues("bad\u0000name", "0123456789ab", "study-note-deploy-system-front", 1.0, 1, 0, "running")   // PostgreSQL 이 받지 않는 글자
        assertThrows(Exception::class.java) {
            store.insertAll(listOf(sample(now, server = "ci-agent-164"), sample(now.plusMillis(1), server = "ci-agent-164", containers = listOf(broken))))
        }
        assertNull(store.lastReceivedOfEveryServer()["ci-agent-164"], "되돌려진 저장의 수신 시각이 남았다")
    }

    // ── latest 의 age_ms · stale ─────────────────────────

    @Test
    fun `측정은 최근이지만 4분 전에 받은 항목과 방금 받은 옛 항목이 있으면 stale 이 아니고 age 는 방금 받은 쪽, 값은 측정이 가장 늦은 항목`() {
        store.insertAll(listOf(
            sample(now.minusSeconds(10), receivedAt = now.minus(Duration.ofMinutes(4)), cpu = 33.0),
            sample(now.minusSeconds(60), receivedAt = now.minusSeconds(1), cpu = 77.0),
        ))
        val response = MetricController(store, "secret", "").latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }, now)
        @Suppress("UNCHECKED_CAST")
        val server = (((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["servers"] as List<Map<String, Any?>>).single()
        assertEquals(1_000L, (server["age_ms"] as Number).toLong(), "age 를 화면 값 항목의 수신 시각으로 셌다")
        assertEquals(false, server["stale"])
        assertEquals(now.minusSeconds(10).toString(), server["at"], "화면 값이 측정 시각이 가장 늦은 항목이 아니다")
        @Suppress("UNCHECKED_CAST")
        assertEquals(33.0, ((server["host"] as Map<String, Any?>)["cpu_pct"] as Number).toDouble())
    }

    // ── 너무 오래된 항목 ─────────────────────────────────

    @Test
    fun `원본이 지워진 구간의 새 항목은 수집 경로에서 버려져 그 구간의 평균이 그대로다`() {
        val bucket = now.minus(Duration.ofDays(8))
        store.insertAll(listOf(sample(bucket.plusSeconds(10), cpu = 10.0), sample(bucket.plusSeconds(40), cpu = 30.0)))
        store.rollUp(now)
        assertEquals(2, store.deleteExpired(now).rawDeleted)
        val stream = FakeMetricStream()
        val collector = MetricCollector(stream, store, block = Duration.ZERO, clock = { now })
        stream.add(MetricRecords.resource("${now.toEpochMilli()}-0", bucket.plusSeconds(100), body = MetricRecords.body(cpuPct = 99.0)))
        collector.runOnce()
        assertEquals(1L, collector.droppedCounts()[MetricDropReason.TOO_OLD])
        store.rollUp(now.plusSeconds(60))
        assertEquals(MetricAggregate(20.0, 30.0, 2), store.rollups("ci-agent-158", MetricScope.HOST, null, bucket, bucket.plus(Duration.ofMinutes(5)))
            .single().values["cpu_pct"], "늦은 한 건이 지워진 구간의 평균을 덮어썼다")
    }

    // ── 5분 평균에서 읽는 구간 경계 ──────────────────────

    @Suppress("UNCHECKED_CAST")
    private fun points(response: ResponseEntity<out Any>) =
        ((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["points"] as List<Map<String, Any?>>

    @Test
    fun `요청 기간과 겹치는 5분 구간은 통째로 한 점이고 시각은 구간의 시작 — to 에서 시작하는 구간은 나오지 않는다`() {
        val day = now.minus(Duration.ofDays(10)).minus(Duration.ofMinutes(30))           // 10일 전 02:30
        store.insertAll(listOf(sample(day.plus(Duration.ofMinutes(3)), cpu = 40.0), sample(day.plus(Duration.ofMinutes(11)), cpu = 90.0)))
        store.rollUp(now)
        store.deleteExpired(now)
        val response = MetricController(store, "secret", "").series(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
            "ci-agent-158", "host", null, day.plus(Duration.ofMinutes(2)).toString(), day.plus(Duration.ofMinutes(10)).toString(), now)
        val found = points(response)
        assertEquals(listOf(day.toString()), found.map { it["at"] }, "겹치는 02:30 구간을 빼먹었거나 to(02:40)에서 시작하는 구간을 넣었다")
        assertEquals(40.0, (found.single()["cpu_pct"] as Number).toDouble())
    }

    // ── 측정 시각 인덱스 ─────────────────────────────────

    @Test
    fun `컨테이너 · GPU 원본 표에 측정 시각으로 시작하는 인덱스가 있다`() {
        for (table in listOf("metric_container", "metric_gpu")) {
            val timeColumns = administrationJdbc.queryForList(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name LIKE 'measured%'",
                String::class.java, schema, table)
            assertTrue(timeColumns.isNotEmpty(), "$table 에 측정 시각 칸이 없다")
            val indexes = administrationJdbc.queryForList("SELECT indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ?", String::class.java, schema, table)
            assertTrue(indexes.any { definition -> timeColumns.any { definition.orEmpty().contains("($it") } },
                "$table 에 측정 시각으로 시작하는 인덱스가 없다: $indexes")
        }
    }
}
