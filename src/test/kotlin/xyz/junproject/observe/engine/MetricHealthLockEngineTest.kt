package xyz.junproject.observe.engine

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.env.Environment
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.json.JsonMapper
import xyz.junproject.observe.metric.MetricRecords
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 실제 앱 — 명세 §16 · §16.1 (리뷰 루프 2): 상태를 정하는 읽기와 선택 칸의 읽기는 서로의 연결을 기다리지 않는다.
 * 선택 칸(서버별 마지막 수신)의 표를 ACCESS EXCLUSIVE 로 잠근 채 `/health` 를 세 번 불러도 매번 2초대에 답하고 `metrics.status` 는 잠그기 전과 같다.
 * 표는 전용 스키마(`health_lock`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class MetricHealthLockEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var redis: StringRedisTemplate

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()

    @Suppress("UNCHECKED_CAST")
    private fun metricsStatus(): Pair<String?, Duration> {
        val started = System.nanoTime()
        val response = http.send(HttpRequest.newBuilder(URI("http://127.0.0.1:${environment.getProperty("local.server.port")}/health"))
            .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString())
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        val metrics = (json.readValue(response.body(), Map::class.java) as Map<String, Any?>)["metrics"] as? Map<String, Any?>
        return metrics?.get("status") as String? to elapsed
    }

    @Test
    fun `선택 칸의 표가 잠겨 있어도 헬스는 세 번 다 2초대에 답하고 지표 status 는 그대로다`() {
        redis.opsForStream<String, String>().add(METRIC_STREAM, MetricRecords.resource("unused", Instant.now().minusSeconds(1)).fields)
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        var before: String? = null
        while (System.nanoTime() < deadline) {
            before = metricsStatus().first
            val stored = administration.queryForObject("SELECT count(*) FROM $SCHEMA.metric_server_receipt", Long::class.java)!!
            if (before == "ok" && stored > 0) break
            Thread.sleep(200)
        }
        assertEquals("ok", before, "잠그기 전의 지표 status 가 ok 가 아니다 — 시험이 성립하지 않는다")

        val locker = administrationPool.connection
        try {
            locker.autoCommit = false
            locker.createStatement().use { it.execute("LOCK TABLE $SCHEMA.metric_server_receipt IN ACCESS EXCLUSIVE MODE") }
            repeat(3) { round ->
                val (status, elapsed) = metricsStatus()
                assertTrue(elapsed < Duration.ofMillis(3_000), "${round + 1}번째 헬스가 ${elapsed.toMillis()}ms 걸렸다")
                assertEquals(before, status, "${round + 1}번째: 선택 칸이 막혀 상태가 바뀌었다(버려진 읽기가 상태 읽기의 연결을 잡았다)")
            }
        } finally {
            runCatching { locker.rollback() }
            locker.close()
        }
    }

    @AfterAll
    fun cleanUp() {
        redis.delete(METRIC_STREAM)
        redis.delete(LOG_STREAM)
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
        administrationPool.close()
    }

    companion object {
        private const val SCHEMA = "health_lock"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val METRIC_STREAM = "health-lock-metrics-$suffix"
        private val LOG_STREAM = "health-lock-logs-$suffix"
        private val administrationPool = EngineSupport.dataSource(size = 2)
        private val administration by lazy { JdbcTemplate(administrationPool) }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
            administration.execute("CREATE SCHEMA $SCHEMA")
            registry.add("observe.db.url") { EngineSupport.databaseUrl + "?currentSchema=$SCHEMA,public" }
            registry.add("observe.db.user") { EngineSupport.databaseUser }
            registry.add("observe.db.password") { EngineSupport.databasePassword }
            registry.add("spring.data.redis.url") { requireNotNull(System.getenv("OBSERVE_TEST_REDIS_URL")) { "OBSERVE_TEST_REDIS_URL 이 없다" } }
            registry.add("observe.stream") { LOG_STREAM }
            registry.add("observe.group") { "health-lock-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.metrics.group") { "health-lock-$suffix" }
            registry.add("observe.secret") { "health-lock-secret" }
        }
    }
}
