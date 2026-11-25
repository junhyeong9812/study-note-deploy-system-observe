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
import xyz.junproject.observe.traffic.TrafficRecords
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 실제 앱(Spring 컨텍스트 전체 + 실제 PostgreSQL · Redis) — 통신 수집이 켜진 상태에서: 새 경로 셋의 인증(입력 검사보다 먼저) · GET 만 · 422,
 * 스트림 `traffic` → 저장 → 지도 · 시계열 · 요청 구간의 한 바퀴, 헬스의 `traffic` 칸과 5분 묶음 · 보관 예약의 배선,
 * 통신 수집의 전용 스레드, 그리고 통신 표가 막혀도 같은 앱의 로그 · 지표 수집과 기존 헬스가 그대로인 것. 표는 전용 스키마(`traffic_http`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class TrafficHttpEngineTest {
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

    private fun <T> eventually(timeout: Duration = Duration.ofSeconds(20), what: String, probe: () -> T?): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            val value = runCatching(probe).getOrNull()
            if (value != null) return value
            Thread.sleep(200)
        }
        fail<Unit>("$what — ${timeout.seconds}초 안에 되지 않았다")
        throw IllegalStateException()
    }

    @Suppress("UNCHECKED_CAST")
    private fun traffic(): Map<String, Any?> = envelope(call("GET", "/health", secret = null))["traffic"] as Map<String, Any?>

    private val paths = listOf("/traffic/map", "/traffic/series?service=backend", "/traffic/requests?request_id=req-1")

    @Test
    fun `새 경로 셋은 비밀값이 없거나 틀리면 401 이고 입력이 틀려도 401 이 먼저다`() {
        for (path in paths + listOf("/traffic/series?class=visit", "/traffic/requests?request_id=bad:id", "/traffic/map?from=x")) {
            for (secret in listOf(null, "wrong")) {
                val response = call("GET", path, secret)
                assertEquals(401, response.statusCode(), "$path secret=$secret ${response.body()}")
                assertEquals(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")), envelope(response), path)
            }
        }
    }

    @Test
    fun `새 경로는 GET 만 받고 입력이 틀리면 422 다`() {
        for (path in paths) for (method in listOf("POST", "PUT", "DELETE")) assertEquals(405, call(method, path).statusCode(), "$method $path")
        for (path in listOf("/traffic/series", "/traffic/series?service=backend&class=visit", "/traffic/requests", "/traffic/requests?request_id=bad:id",
                "/traffic/map?from=yesterday")) {
            val response = call("GET", path)
            assertEquals(422, response.statusCode(), "$path ${response.body()}")
            @Suppress("UNCHECKED_CAST")
            assertEquals("invalid_request", (envelope(response)["error"] as Map<String, Any?>)["code"], path)
        }
    }

    @Test
    fun `스트림에 넣은 통신 항목이 수집되어 지도 · 시계열 · 요청 구간과 헬스의 traffic 칸으로 나온다`() {
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val cell = TrafficRecords.cellStart(now).minusSeconds(60)
        val requestId = "req-${UUID.randomUUID().toString().take(12)}"
        val generation = UUID.randomUUID().toString().replace("-", "").take(16)
        for (sequence in 1L..2L) {
            val body = TrafficRecords.body(
                listOf(TrafficRecords.segment(cell, listOf(200, 502), durationsMs = listOf(30, 700))),
                if (sequence == 1L) listOf(TrafficRecords.request(cell.plusSeconds(1), requestId = requestId, status = 502, durationMs = 700)) else emptyList(),
            )
            redis.opsForStream<String, String>().add(TRAFFIC_STREAM, TrafficRecords.fields(sequence, now.minusSeconds(2 - sequence), body, server = "ci-agent-http", generation = generation))
        }
        val span = eventually(what = "요청 구간이 조회된다") {
            @Suppress("UNCHECKED_CAST")
            (data(call("GET", "/traffic/requests?request_id=$requestId"))["spans"] as List<Map<String, Any?>>).singleOrNull()
        }
        assertEquals("ci-agent-http", span["server"])
        assertEquals(502, span["status"])

        // 두 항목이 모두 들어가면 읽기 진도는 순번 2 의 at(= now)이다
        val map = eventually(what = "두 항목이 모두 저장된다") {
            data(call("GET", "/traffic/map?from=${cell.minusSeconds(30)}&to=${now.plusSeconds(1)}")).takeIf { answer ->
                @Suppress("UNCHECKED_CAST")
                (answer["servers"] as List<Map<String, Any?>>).any { it["server"] == "ci-agent-http" && it["read_progress_at"] == now.toString() }
            }
        }
        @Suppress("UNCHECKED_CAST")
        val server = (map["servers"] as List<Map<String, Any?>>).single { it["server"] == "ci-agent-http" }
        assertEquals(0, (server["sequence_gaps"] as Number).toInt())
        assertEquals(false, server["receive_delayed"])
        assertEquals(1, (server["first_seq_seen"] as Number).toInt())
        assertEquals(0, (server["requests_rejected"] as Number).toInt())
        @Suppress("UNCHECKED_CAST")
        val edge = (map["edges"] as List<Map<String, Any?>>).single { it["service"] == "backend" && it["caller"] == "front" }
        assertEquals(4, (edge["count"] as Number).toInt() + (edge["filling_count"] as Number).toInt())

        val series = data(call("GET", "/traffic/series?service=backend&caller=front&from=${cell.minusSeconds(30)}&to=${now.plusSeconds(1)}"))
        @Suppress("UNCHECKED_CAST")
        val point = (series["points"] as List<Map<String, Any?>>).single()
        assertEquals(cell.toString(), point["at"])
        assertEquals(4, (point["count"] as Number).toInt())
        assertEquals(0.5, (point["error_rate"] as Number).toDouble())

        val health = traffic()
        assertTrue((health["stored_since_start"] as Number).toLong() >= 2, "$health")
        for (field in listOf("status", "dropped", "duplicates", "cells_dropped", "at_ahead", "at_too_old", "requests_rejected", "last_store_failure", "servers", "unacknowledged", "lag"))
            assertTrue(health.containsKey(field), "헬스의 traffic 에 $field 가 없다: $health")
    }

    @Test
    fun `통신 5분 묶음과 보관 삭제가 예약 실행에 배선되어 헬스에 결과가 남는다`() {
        val health = eventually(Duration.ofSeconds(15), "5분 묶음 · 보관의 결과가 헬스에 남는다") {
            traffic().takeIf { it["last_rollup_at"] != "" && it["last_retention_at"] != "" }
        }
        assertEquals("", health["last_rollup_failure"])
        assertEquals("", health["last_retention_failure"])
    }

    @Test
    fun `통신 수집은 전용 스레드와 전용 연결 풀로 돈다`() {
        val names = Thread.getAllStackTraces().keys.map { it.name }
        assertTrue(names.contains("observe-traffic-collector"), "통신 수집 스레드가 없다: $names")
        assertTrue(names.any { it.startsWith("observe-traffic-ingest") }, "통신 수집 전용 연결 풀이 없다: $names")
    }

    @Test
    fun `통신 표가 막혀 통신 수집이 멈춰도 같은 앱의 로그 · 지표 수집과 기존 헬스의 칸은 그대로다`() {
        eventually(what = "통신 표가 만들어진다") { administration.queryForObject("SELECT to_regclass('$SCHEMA.traffic_receipt')::text", String::class.java) }
        val lock: Connection = administrationSource.connection
        lock.autoCommit = false
        try {
            lock.createStatement().use { it.execute("LOCK TABLE $SCHEMA.traffic_receipt IN ACCESS EXCLUSIVE MODE") }
            val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
            redis.opsForStream<String, String>().add(TRAFFIC_STREAM,
                TrafficRecords.fields(1, now, TrafficRecords.body(), server = "ci-agent-blocked", generation = UUID.randomUUID().toString().replace("-", "").take(16)))
            val requestId = "req-blocked-${UUID.randomUUID().toString().take(8)}"
            redis.opsForStream<String, String>().add(LOG_STREAM, mapOf("level" to "info", "line" to "$requestId:backend:still collecting"))
            redis.opsForStream<String, String>().add(METRIC_STREAM, MetricRecords.resource("unused", now.minusSeconds(1), server = "ci-agent-blocked").fields)
            eventually(what = "통신 표가 막힌 동안 로그가 저장된다") {
                @Suppress("UNCHECKED_CAST")
                (data(call("GET", "/logs?request_id=$requestId"))["logs"] as List<Any?>).singleOrNull()
            }
            eventually(what = "통신 표가 막힌 동안 지표가 저장된다") {
                @Suppress("UNCHECKED_CAST")
                (data(call("GET", "/metrics/latest"))["servers"] as List<Map<String, Any?>>).singleOrNull { it["server"] == "ci-agent-blocked" && it["at"] != null }
            }
            val health = envelope(call("GET", "/health", secret = null))
            for (field in listOf("status", "last_loop_at", "unacknowledged", "metrics", "last_retention_at", "traffic")) assertTrue(health.containsKey(field), "헬스에 $field 가 없다")
        } finally {
            runCatching { lock.rollback() }
            lock.close()
        }
        eventually(what = "막힘이 풀리면 통신 항목이 저장된다") {
            @Suppress("UNCHECKED_CAST")
            (data(call("GET", "/traffic/map"))["servers"] as List<Map<String, Any?>>).singleOrNull { it["server"] == "ci-agent-blocked" }
        }
    }

    @AfterAll
    fun dropSchema() {
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    companion object {
        private const val SCHEMA = "traffic_http"
        private const val SECRET = "traffic-secret"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val TRAFFIC_STREAM = "traffic-http-$suffix"
        private val LOG_STREAM = "traffic-http-logs-$suffix"
        private val METRIC_STREAM = "traffic-http-metrics-$suffix"
        private val administrationSource by lazy { EngineSupport.dataSource(size = 2) }
        private val administration by lazy { JdbcTemplate(administrationSource) }

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
            registry.add("observe.group") { "traffic-http-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.traffic.stream") { TRAFFIC_STREAM }
            registry.add("observe.traffic.group") { "traffic-http-$suffix" }
            registry.add("observe.traffic.rollup-interval-ms") { "300" }
            registry.add("observe.retention-cron") { "* * * * * *" }
            registry.add("observe.secret") { SECRET }
        }
    }
}
