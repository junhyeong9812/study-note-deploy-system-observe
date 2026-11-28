package xyz.junproject.observe.engine

import com.sun.net.httpserver.HttpServer
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
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 앱(Spring 컨텍스트 전체 + 실제 PostgreSQL · Redis) + 알림 서버 대역(JDK HTTP 서버) — 명세 §2.4 · §11.4 의 배선:
 * 예약된 판정이 실제 표본으로 넘음을 찾아 `POST /events` 로 보낸다 · 알림 서버가 처음에 받지 못하면(503) 남겨 두었다가 다시 보낸다 ·
 * 수치는 설정(`observe.metrics.alert.*`)으로 바뀐다 · 기대 서버의 끊김 · `firing` · 헬스의 판정 칸 ·
 * 알림 서버가 받지 못하는 동안에도 로그 · 헬스의 기존 판정은 그대로. 표는 전용 스키마(`threshold_http`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext   // 클래스가 끝나면 문맥(연결 풀 19개)을 닫는다 — 문맥이 쌓이면 테스트 DB 의 연결 상한(100)을 넘는다
class ThresholdHttpEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var redis: StringRedisTemplate

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"

    private fun call(path: String, secret: String? = SECRET, callerService: String? = null): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("$base$path")).timeout(Duration.ofSeconds(10)).GET()
        if (secret != null) builder.header("X-Observe-Secret", secret)
        if (callerService != null) builder.header("X-Caller-Service", callerService)
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    @Suppress("UNCHECKED_CAST")
    private fun body(response: HttpResponse<String>) = json.readValue(response.body(), Map::class.java) as Map<String, Any?>

    @Suppress("UNCHECKED_CAST")
    private fun data(response: HttpResponse<String>): Map<String, Any?> {
        assertEquals(200, response.statusCode(), response.body())
        return body(response)["data"] as Map<String, Any?>
    }

    private fun <T> eventually(what: String, timeout: Duration = Duration.ofSeconds(30), probe: () -> T?): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            runCatching(probe).getOrNull()?.let { return it }
            Thread.sleep(200)
        }
        fail<Unit>("$what — ${timeout.seconds}초 안에 되지 않았다")
        throw IllegalStateException()
    }

    /** 메모리 [memoryPct]% 인 항목을 측정 시각 [at] 으로 넣는다 — agent 가 보내는 꼴 그대로 */
    private fun sendMetric(at: Instant, memoryPct: Int, server: String = "ci-agent-158") {
        val used = 16_000_000_000L / 100 * memoryPct
        val body = """{"interval_ms": 30000, "host": {"cpu_pct": 5, "load1": 0.1, "mem_total": 16000000000, "mem_used": $used,
            "disk_total": 1000, "disk_used": 100, "net_rx_bps": 1, "net_tx_bps": 1, "io_read_bps": 0, "io_write_bps": 0},
            "containers": [], "gpus": [], "missing": []}"""
        redis.opsForStream<String, String>().add(METRIC_STREAM, mapOf("kind" to "resource", "v" to "1", "server" to server,
            "at" to at.toEpochMilli().toString(), "body" to body))
    }

    /** 5분 창을 덮는 열 개(30초 간격) — 설정한 메모리 기준(50%)을 넘는 60% */
    private fun sendFiveMinutesOfHighMemory() {
        val newest = Instant.now().minusSeconds(1)
        (9 downTo 0).forEach { sendMetric(newest.minusSeconds(30L * it), 60) }
    }

    @Test
    fun `알림 서버로 가는 모든 호출은 처음에 받지 못해 다시 보낸 것까지 X-Caller-Service observe 하나를 싣는다`() {
        // 들어온 요청이 다른 이름을 실어도(어드민의 조회) 나가는 호출은 자기 이름이다
        assertEquals(200, call("/metrics/latest", callerService = "admin").statusCode(), "준비: 조회")
        eventually("메모리 알림을 다시 보내 받게 된다") {
            received.firstOrNull { it.key == "resource:ci-agent-158:memory" && it.status == 202 }
        }
        val memoryCalls = received.filter { it.key == "resource:ci-agent-158:memory" }
        assertTrue(memoryCalls.size >= 3, "준비: 받지 못한 두 번과 다시 보낸 호출이 없다 ${memoryCalls.map { it.status }}")
        received.forEach { request ->
            assertEquals(listOf("observe"), request.callers, "${request.key} (응답 ${request.status}): X-Caller-Service 가 정확히 하나의 observe 가 아니다")
        }
    }

    @Test
    fun `예약된 판정이 설정한 수치로 넘음을 찾아 알림 서버로 보내고 처음에 받지 못한 것은 다시 보낸다`() {
        sendFiveMinutesOfHighMemory()
        val accepted = eventually("알림 서버가 메모리 넘음을 202 로 받기") {
            received.firstOrNull { it.key == "resource:ci-agent-158:memory" && it.status == 202 }
        }
        assertEquals("alert-secret", accepted.secret)
        @Suppress("UNCHECKED_CAST")
        val event = json.readValue(accepted.body, Map::class.java) as Map<String, Any?>
        assertEquals("observe", event["source"])
        assertEquals("resource_memory", event["kind"])
        assertEquals("warning", event["severity"])
        assertTrue((event["title"] as String).isNotBlank())
        val attempts = received.count { it.key == "resource:ci-agent-158:memory" }
        assertTrue(attempts >= 3, "503 두 번 뒤에 다시 보내야 받는다 — 보낸 횟수 $attempts")

        // 넘음이 이어져도 다시 보내지 않는다(1시간 안)
        Thread.sleep(2_000)
        assertEquals(1, received.count { it.key == "resource:ci-agent-158:memory" && it.status == 202 }, "같은 넘음을 또 보냈다")
    }

    @Test
    fun `한 번도 보내지 않은 기대 서버의 끊김을 알린다`() {
        eventually("알림 서버가 ci-agent-164 의 끊김을 받기") {
            received.firstOrNull { it.key == "resource:ci-agent-164:silent" && it.status == 202 }
        }
    }

    @Test
    fun `한 번도 보내지 않은 기대 서버의 끊김이 latest 의 firing 에 나온다`() {
        val silent = eventually("firing 에 ci-agent-164 의 끊김이 나오기") {
            @Suppress("UNCHECKED_CAST")
            (data(call("/metrics/latest"))["firing"] as List<Map<String, Any?>>).firstOrNull { it["server"] == "ci-agent-164" && it["rule"] == "silent" }
        }
        assertEquals("warning", silent["severity"])
    }

    @Test
    fun `latest 의 firing 에 지금 넘어 있는 규칙이 나온다`() {
        sendFiveMinutesOfHighMemory()
        val memory = eventually("firing 에 메모리가 나오기") {
            @Suppress("UNCHECKED_CAST")
            (data(call("/metrics/latest"))["firing"] as List<Map<String, Any?>>).firstOrNull { it["server"] == "ci-agent-158" && it["rule"] == "memory" }
        }
        assertEquals(setOf("server", "rule", "target", "severity", "since", "last_value"), memory.keys)
        assertEquals("warning", memory["severity"])
        assertEquals(60.0, (memory["last_value"] as Number).toDouble(), 0.5)
        Instant.parse(memory["since"] as String)
    }

    @Test
    fun `헬스의 지표 칸에 마지막 판정 시각과 못 보낸 알림 수가 나오고 로그 수집의 판정은 그대로다`() {
        val metrics = eventually("헬스에 last_judged_at 이 차기") {
            @Suppress("UNCHECKED_CAST")
            (body(call("/health", secret = null))["metrics"] as? Map<String, Any?>)?.takeIf { (it["last_judged_at"] as? String)?.isNotEmpty() == true }
        }
        for (field in listOf("last_judged_at", "last_judge_failure", "last_dispatch_failure", "undelivered_alerts", "rejected_alerts")) {
            assertTrue(metrics.containsKey(field), "metrics.$field 이 없다: $metrics")
        }
        val health = eventually("헬스가 200 ok") { call("/health", secret = null).takeIf { it.statusCode() == 200 } }
        assertEquals("ok", body(health)["status"])
    }

    @Test
    fun `알림 서버가 받지 못하는 동안에도 로그는 수집되고 조회된다`() {
        val marker = "threshold-http-${UUID.randomUUID()}"
        redis.opsForStream<String, String>().add(LOG_STREAM, mapOf("level" to "info", "line" to "req-1:backend:$marker"))
        eventually("로그가 /logs 에 나오기") {
            @Suppress("UNCHECKED_CAST")
            (data(call("/logs?q=$marker"))["logs"] as List<Map<String, Any?>>).firstOrNull()
        }
    }

    @AfterAll
    fun cleanUp() {
        alertServer.stop(0)
        redis.delete(METRIC_STREAM)
        redis.delete(LOG_STREAM)
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    private class Received(val key: String?, val secret: String?, val body: String, val status: Int, val callers: List<String>)

    companion object {
        private const val SCHEMA = "threshold_http"
        private const val SECRET = "threshold-secret"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val METRIC_STREAM = "threshold-http-metrics-$suffix"
        private val LOG_STREAM = "threshold-http-logs-$suffix"
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }
        private val json = JsonMapper.builder().build()

        /** 알림 서버 대역 — 메모리 알림은 처음 두 번 503(받지 못함), 그 뒤로 202. 그 밖은 늘 202 */
        private val received = CopyOnWriteArrayList<Received>()
        private val memoryAttempts = AtomicInteger()
        private val alertServer: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/events") { exchange ->
                val text = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8)
                @Suppress("UNCHECKED_CAST")
                val key = runCatching { (json.readValue(text, Map::class.java) as Map<String, Any?>)["key"] as String? }.getOrNull()
                val status = if (key == "resource:ci-agent-158:memory" && memoryAttempts.incrementAndGet() <= 2) 503 else 202
                received += Received(key, exchange.requestHeaders.getFirst("X-Alert-Secret"), text, status, exchange.requestHeaders["X-Caller-Service"].orEmpty())
                val answer = """{"success":true,"data":{"sent":true,"key":"$key"}}""".toByteArray()
                exchange.sendResponseHeaders(status, answer.size.toLong())
                exchange.responseBody.use { it.write(answer) }
            }
            executor = Executors.newCachedThreadPool()
            start()
        }

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
            registry.add("observe.group") { "threshold-http-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.metrics.group") { "threshold-http-$suffix" }
            registry.add("observe.metrics.expected-servers") { "ci-agent-164" }
            registry.add("observe.metrics.judge-interval-ms") { "300" }
            registry.add("observe.alert.dispatch-interval-ms") { "300" }               // 보내기는 판정과 따로 도는 예약(§15) — 다시 보내기가 30초 안에 보이게
            registry.add("observe.metrics.alert.silent-warm-up") { "1s" }               // 끊김 판정의 시작(§15.1)을 짧게 — 기본 3분
            registry.add("observe.metrics.alert.memory-pct") { "50" }
            registry.add("observe.metrics.alert.memory-resolve-pct") { "40" }
            registry.add("observe.alert.events-url") { "http://127.0.0.1:${alertServer.address.port}/events" }
            registry.add("observe.alert.secret") { "alert-secret" }
            registry.add("observe.secret") { SECRET }
        }
    }
}
