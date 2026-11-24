package xyz.junproject.observe.engine

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.core.env.Environment
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
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
 * 실제 앱(Spring 컨텍스트 전체 + 실제 PostgreSQL·Redis) — 지표 수집기가 켜진 상태에서:
 * 새 경로 셋의 인증(입력 검사보다 먼저)·GET 만·422, 스트림 → 저장 → 조회의 한 바퀴, 헬스의 `metrics` 칸과 5분 평균 예약,
 * 그리고 **기존 동작 무변경**(로그 수집·`/logs`·`/logs/stats`·`/auth/…`·헬스의 기존 칸 — 지표 스트림에 쓰레기가 있어도).
 * 표는 전용 스키마(`metric_http`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext   // 클래스가 끝나면 문맥(연결 풀 19개)을 닫는다 — 문맥이 쌓이면 테스트 DB 의 연결 상한(100)을 넘는다
class MetricHttpEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var redis: StringRedisTemplate

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"

    private fun call(method: String, path: String, secret: String? = SECRET): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("$base$path")).timeout(Duration.ofSeconds(10))
        if (secret != null) builder.header("X-Observe-Secret", secret)
        builder.method(method, HttpRequest.BodyPublishers.noBody())
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    @Suppress("UNCHECKED_CAST")
    private fun envelope(response: HttpResponse<String>): Map<String, Any?> = try {
        json.readValue(response.body(), Map::class.java) as Map<String, Any?>
    } catch (error: Exception) {
        fail("봉투가 JSON 이 아니다: status=${response.statusCode()} body=${response.body()}")
    }

    @Suppress("UNCHECKED_CAST")
    private fun data(response: HttpResponse<String>): Map<String, Any?> {
        assertEquals(200, response.statusCode(), response.body())
        return envelope(response)["data"] as Map<String, Any?>
    }

    private fun sendMetric(fields: Map<String, String>) = redis.opsForStream<String, String>().add(METRIC_STREAM, fields)!!.value
    private fun sendLog(line: String) = redis.opsForStream<String, String>().add(LOG_STREAM, mapOf("level" to "info", "line" to line))!!.value

    /** 조건이 맞을 때까지 기다린다(최대 [timeout]) — 수집 스레드·예약 실행은 실제 시간으로 돈다 */
    private fun <T> eventually(timeout: Duration = Duration.ofSeconds(20), what: String, probe: () -> T?): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        var last: T? = null
        while (System.nanoTime() < deadline) {
            last = runCatching(probe).getOrNull()
            if (last != null) return last
            Thread.sleep(200)
        }
        fail<Unit>("$what — ${timeout.seconds}초 안에 되지 않았다")
        throw IllegalStateException()
    }

    @Suppress("UNCHECKED_CAST")
    private fun health() = envelope(call("GET", "/health", secret = null))

    private val metricPaths = listOf("/metrics/latest", "/metrics/series?server=ci-agent-158&scope=host", "/metrics/containers?server=ci-agent-158")

    @Test
    fun `새 경로 셋은 비밀값이 없거나 틀리면 401 이고 입력이 틀려도 401 이 먼저다`() {
        val unauthorized = mapOf("success" to false, "error" to mapOf("code" to "unauthorized"))
        for (path in metricPaths + listOf("/metrics/series?scope=nope&from=x", "/metrics/containers?at=x")) {
            for (secret in listOf(null, "wrong")) {
                val response = call("GET", path, secret)
                assertEquals(401, response.statusCode(), "$path secret=$secret ${response.body()}")
                assertEquals(unauthorized, envelope(response), path)
            }
        }
    }

    @Test
    fun `새 경로는 GET 만 받는다`() {
        for (path in metricPaths) {
            for (method in listOf("POST", "PUT", "DELETE")) {
                val response = call(method, path)
                assertEquals(405, response.statusCode(), "$method $path ${response.body()}")
            }
        }
    }

    @Test
    fun `입력이 틀리면 422 invalid_request 봉투`() {
        for (path in listOf("/metrics/series", "/metrics/series?server=ci-agent-158&scope=disk", "/metrics/series?server=ci-agent-158&scope=host&from=yesterday",
                "/metrics/series?server=ci-agent-158&scope=container", "/metrics/containers", "/metrics/containers?server=ci-agent-158&at=x")) {
            val response = call("GET", path)
            assertEquals(422, response.statusCode(), "$path ${response.body()}")
            @Suppress("UNCHECKED_CAST")
            assertEquals("invalid_request", (envelope(response)["error"] as Map<String, Any?>)["code"], path)
        }
    }

    @Test
    fun `스트림에 넣은 지표가 수집되어 latest · series · containers 로 나온다`() {
        val measuredAt = Instant.now().minusSeconds(2)
        sendMetric(MetricRecords.resource("unused", measuredAt, "ci-agent-158").fields)
        val server = eventually(what = "latest 에 ci-agent-158 이 나오기") {
            @Suppress("UNCHECKED_CAST")
            (data(call("GET", "/metrics/latest"))["servers"] as List<Map<String, Any?>>).firstOrNull { it["server"] == "ci-agent-158" }
        }
        assertEquals(Instant.ofEpochMilli(measuredAt.toEpochMilli()).toString(), server["at"])
        assertEquals(false, server["stale"])
        @Suppress("UNCHECKED_CAST")
        assertEquals(emptyList<Any>(), (data(call("GET", "/metrics/latest"))["firing"] as List<Map<String, Any?>>).filter { it["server"] == "ci-agent-158" })

        val series = data(call("GET", "/metrics/series?server=ci-agent-158&scope=host"))
        assertEquals("raw", series["source"])
        @Suppress("UNCHECKED_CAST")
        assertEquals(3.4, ((series["points"] as List<Map<String, Any?>>).single()["cpu_pct"] as Number).toDouble())

        val containers = data(call("GET", "/metrics/containers?server=ci-agent-158"))
        @Suppress("UNCHECKED_CAST")
        assertEquals(listOf("front-app"), (containers["containers"] as List<Map<String, Any?>>).map { it["name"] })
    }

    @Test
    fun `뜬 직후에는(기본 3분) 한 번도 안 보낸 기대 서버가 있어도 끊김을 알리지 않고 헬스의 silent_judging 이 false 다`() {
        @Suppress("UNCHECKED_CAST")
        val firing = data(call("GET", "/metrics/latest"))["firing"] as List<Map<String, Any?>>
        assertFalse(firing.any { it["server"] == "ci-agent-164" && it["rule"] == "silent" }, "뜬 직후에 끊김을 알렸다: $firing")
        @Suppress("UNCHECKED_CAST")
        val metrics = envelope(call("GET", "/health", secret = null))["metrics"] as Map<String, Any?>
        assertEquals(false, metrics["silent_judging"], "뜬 직후인데 끊김을 판정하는 중이다: $metrics")
    }

    @Test
    fun `기대 서버로 설정한 서버는 한 번도 보내지 않았어도 수신 없음으로 나온다`() {
        @Suppress("UNCHECKED_CAST")
        val silent = (data(call("GET", "/metrics/latest"))["servers"] as List<Map<String, Any?>>).firstOrNull { it["server"] == "ci-agent-164" }
            ?: fail("설정(METRICS_EXPECTED_SERVERS)의 서버가 나오지 않는다")
        assertNull(silent["at"])
        assertEquals(true, silent["stale"])
    }

    @Test
    fun `헬스에 지표 칸이 나오고 깨진 항목은 이유별로 세며 5분 평균이 예약대로 돈다`() {
        sendMetric(mapOf("kind" to "resource", "v" to "1", "server" to "ci-agent-158", "at" to Instant.now().toEpochMilli().toString(), "body" to "{"))
        val metrics = eventually(what = "헬스의 metrics.dropped.body_not_json 이 1 이상") {
            @Suppress("UNCHECKED_CAST")
            (health()["metrics"] as? Map<String, Any?>)?.takeIf { (((it["dropped"] as? Map<String, Any?>)?.get("body_not_json") as? Number)?.toLong() ?: 0L) >= 1 }
        }
        for (field in listOf("status", "last_stored_at", "dropped", "dropped_total", "unacknowledged", "lag", "last_rollup_at", "last_rollup_failure",
                "last_retention_at", "last_retention_raw_deleted", "last_retention_rollup_deleted", "last_retention_failure", "last_received_at")) {
            assertTrue(metrics.containsKey(field), "metrics.$field 이 없다: $metrics")
        }
        eventually(what = "5분 평균 예약이 한 번 성공하기(last_rollup_at)") {
            @Suppress("UNCHECKED_CAST")
            ((health()["metrics"] as? Map<String, Any?>)?.get("last_rollup_at") as? String)?.takeIf { it.isNotEmpty() }
        }
    }

    // ── 기존 동작 무변경(특성) ───────────────────────────

    @Test
    fun `지표 스트림에 쓰레기가 있어도 로그는 수집되어 logs 로 나온다`() {
        repeat(20) { sendMetric(mapOf("garbage" to "x".repeat(2_000))) }
        val marker = "metric-http-${UUID.randomUUID()}"
        sendLog("req-1:backend:$marker")
        eventually(what = "로그가 수집되어 /logs 에 나오기") {
            @Suppress("UNCHECKED_CAST")
            (data(call("GET", "/logs?q=$marker"))["logs"] as List<Map<String, Any?>>).firstOrNull()
        }.let { assertEquals("backend", it["service"]) }
    }

    @Test
    fun `기존 경로는 그대로 답한다`() {
        assertEquals(401, call("GET", "/logs", secret = null).statusCode())
        assertEquals(200, call("GET", "/logs").statusCode())
        assertEquals(200, call("GET", "/logs/stats").statusCode())
        assertEquals(422, call("GET", "/logs?limit=many").statusCode())
        assertEquals(200, call("GET", "/auth/lock").statusCode())
        assertEquals(200, call("GET", "/auth/attempts").statusCode())
    }

    @Test
    fun `헬스의 기존 칸은 그대로 있고 로그 수집이 돌면 200 ok 다`() {
        val body = eventually(what = "헬스가 200 ok") {
            call("GET", "/health", secret = null).takeIf { it.statusCode() == 200 }?.let { envelope(it) }
        }
        assertEquals("ok", body["status"])
        for (field in listOf("status", "last_loop_at", "last_stored_at", "stored_since_start", "unacknowledged", "lag", "failure",
                "last_retention_at", "last_retention_deleted", "last_retention_failure",
                "last_auth_retention_at", "last_auth_retention_deleted", "last_auth_retention_failure")) {
            assertTrue(body.containsKey(field), "기존 칸 $field 이 없다: $body")
        }
    }

    @AfterAll
    fun cleanUp() {
        redis.delete(METRIC_STREAM)
        redis.delete(LOG_STREAM)
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    companion object {
        private const val SCHEMA = "metric_http"
        private const val SECRET = "metric-secret"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val METRIC_STREAM = "metric-http-metrics-$suffix"
        private val LOG_STREAM = "metric-http-logs-$suffix"
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }

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
            registry.add("observe.group") { "metric-http-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.metrics.group") { "metric-http-$suffix" }
            registry.add("observe.metrics.expected-servers") { "ci-agent-164" }
            registry.add("observe.metrics.rollup-interval-ms") { "300" }
            registry.add("observe.secret") { SECRET }
        }
    }
}
