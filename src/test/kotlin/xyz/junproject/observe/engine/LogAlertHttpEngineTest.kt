package xyz.junproject.observe.engine

import com.sun.net.httpserver.HttpServer
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.core.env.Environment
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.json.JsonMapper
import xyz.junproject.observe.ObserveConfiguration
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * 실제 앱 — 로그 무수신 판정이 설정(`observe.log-alerts.services` = `LOG_ALERT_SERVICES`)으로 켜져 예약된 판정(자원 판정과 같은 주기 `observe.metrics.judge-interval-ms`)이
 * 한 번도 로그를 보내지 않은 서비스의 무수신을 알림 서버로 보낸다 · 사는 서버가 자원 끊김으로 넘어 있는 서비스는 보내지 않는다 · 거절 · 감시하지 않는 줄이 기동 로그에 드러난다 ·
 * 헬스 본문에 `log_alerts` 칸이 나오고 수집 헬스는 200 ok 그대로 · 연결 풀은 그대로(판정 · 알림 풀을 함께 쓴다).
 * 판정의 시작(`observe.log-alerts.warm-up`, 기본 3m)은 짧게 둔다. 표는 전용 스키마(`log_alert_http`).
 */
@Tag("engine")
@ExtendWith(OutputCaptureExtension::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class LogAlertHttpEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var configuration: ObserveConfiguration

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"

    @Suppress("UNCHECKED_CAST")
    private fun health(): Pair<Int, Map<String, Any?>> {
        val response = http.send(HttpRequest.newBuilder(URI("$base/health")).timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString())
        return response.statusCode() to (json.readValue(response.body(), Map::class.java) as Map<String, Any?>)
    }

    private fun <T> eventually(what: String, timeout: Duration = Duration.ofSeconds(40), probe: () -> T?): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            runCatching(probe).getOrNull()?.let { return it }
            Thread.sleep(200)
        }
        fail<Unit>("$what — ${timeout.seconds}초 안에 되지 않았다")
        throw IllegalStateException()
    }

    @Test
    fun `한 번도 로그를 보내지 않은 서비스의 무수신이 알림 서버로 가고 사는 서버가 끊긴 서비스는 가지 않는다`() {
        val ghost = eventually("알림 서버가 ghost 의 무수신을 받기") { received.firstOrNull { it.key == "log:ghost:silent" } }
        @Suppress("UNCHECKED_CAST")
        val event = json.readValue(ghost.body, Map::class.java) as Map<String, Any?>
        assertEquals("log_silent", event["kind"])
        assertEquals("warning", event["severity"])
        eventually("알림 서버가 ci-agent-164 의 자원 끊김을 받기") { received.firstOrNull { it.key == "resource:ci-agent-164:silent" } }
        Thread.sleep(3_000)
        assertEquals(emptyList<String?>(), received.map { it.key }.filter { it == "log:backend:silent" }, "사는 서버가 끊겼는데 로그 무수신을 보냈다")
        assertEquals(1, received.count { it.key == "log:ghost:silent" }, "같은 넘음을 1시간 안에 다시 보냈다")
    }

    @Test
    fun `헬스 본문에 log_alerts 칸이 나오고 수집 헬스는 200 ok 그대로다`() {
        @Suppress("UNCHECKED_CAST")
        val section = eventually("헬스의 log_alerts.last_judged_at 이 차기") {
            (health().second["log_alerts"] as? Map<String, Any?>)?.takeIf { (it["last_judged_at"] as? String)?.isNotEmpty() == true }
        }
        assertEquals(true, section["enabled"])
        assertEquals(2, (section["watched_services"] as Number).toInt(), "감시하는 서비스는 ghost · backend 둘이다: $section")
        assertEquals(false, section["watch_all"])
        assertEquals(true, section["judging"])
        assertEquals("", section["last_failure"])
        val (status, body) = eventually("헬스가 200") { health().takeIf { it.first == 200 } }
        assertEquals(200, status)
        assertEquals("ok", body["status"])
    }

    @Test
    fun `거절한 줄과 감시하지 않는 줄이 기동 로그에 드러난다`(output: CapturedOutput) {
        assertTrue(output.all.contains("name_reserved"), "별표 이름의 거절이 기동 로그에 없다")
        assertTrue(output.all.contains("threshold_over_6h"), "6시간을 넘는 기준이 기동 로그에 없다")
    }

    @Test
    fun `로그 무수신 판정을 켜도 앱의 연결 풀은 그대로다`() {
        val field = ObserveConfiguration::class.java.getDeclaredField("pools").apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        val pools = (field.get(configuration) as List<HikariDataSource>).associate { it.poolName to it.maximumPoolSize }
        assertEquals(21, pools.values.sum(), "자리당 연결 수가 바뀌었다: $pools")
        assertEquals(9, pools.size, "새 풀이 생겼다: $pools")
    }

    private class Received(val key: String?, val body: String)

    companion object {
        private const val SCHEMA = "log_alert_http"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }
        private val json = JsonMapper.builder().build()
        private val received = CopyOnWriteArrayList<Received>()
        private val alertServer: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/events") { exchange ->
                val text = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8)
                @Suppress("UNCHECKED_CAST")
                val key = runCatching { (json.readValue(text, Map::class.java) as Map<String, Any?>)["key"] as String? }.getOrNull()
                received += Received(key, text)
                val answer = """{"success":true,"data":{"sent":true,"key":"$key"}}""".toByteArray()
                exchange.sendResponseHeaders(202, answer.size.toLong())
                exchange.responseBody.use { it.write(answer) }
            }
            executor = Executors.newCachedThreadPool()
            start()
        }

        @JvmStatic
        @AfterAll
        fun cleanUp() {
            alertServer.stop(0)
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
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
            registry.add("observe.stream") { "log-alert-http-logs-$suffix" }
            registry.add("observe.group") { "log-alert-http-$suffix" }
            registry.add("observe.metrics.stream") { "log-alert-http-metrics-$suffix" }
            registry.add("observe.metrics.group") { "log-alert-http-$suffix" }
            registry.add("observe.metrics.expected-servers") { "ci-agent-164" }
            registry.add("observe.metrics.judge-interval-ms") { "300" }
            registry.add("observe.alert.dispatch-interval-ms") { "300" }
            registry.add("observe.metrics.alert.silent-warm-up") { "1s" }
            registry.add("observe.log-alerts.warm-up") { "4s" }                     // 자원 끊김(1s)이 먼저 상태에 남은 뒤에 로그 판정이 시작되게
            registry.add("observe.log-alerts.services") { "ghost@ci-agent-158=5,*@ci-agent-9=5,front@ci-agent-9=400,backend@ci-agent-164=5" }
            registry.add("observe.alert.events-url") { "http://127.0.0.1:${alertServer.address.port}/events" }
            registry.add("observe.alert.secret") { "alert-secret" }
            registry.add("observe.secret") { "log-alert-http-secret" }
        }
    }
}
