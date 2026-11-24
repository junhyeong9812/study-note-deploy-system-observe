package xyz.junproject.observe.engine

import com.sun.net.httpserver.HttpServer
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
import xyz.junproject.observe.threshold.ThresholdRound
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 앱(실제 예약 실행기) — 명세 §15 · §15.1 (리뷰 루프 1): 보내기와 판정은 서로 기다리지 않는다. 알림 서버가 한 건에 3초씩 걸리는 동안에도
 * 판정은 제 주기(이 시험에서는 200ms)에 계속 돈다 — 헬스의 `metrics.last_judged_at` 이 보내는 도중에도 바뀐다.
 * 알림 서버 대역은 JDK HTTP 서버(한 건에 3초), 넘음은 메모리 기준을 50% 로 낮추고 서버 셋의 지표로 만든다. 표는 전용 스키마(`dispatch_schedule`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class AlertDispatchScheduleEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var redis: StringRedisTemplate
    @Autowired private lateinit var thresholdRound: ThresholdRound

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"

    @Suppress("UNCHECKED_CAST")
    private fun lastJudgedAt(): String? {
        val response = http.send(HttpRequest.newBuilder(URI("$base/health")).timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString())
        val metrics = (json.readValue(response.body(), Map::class.java) as Map<String, Any?>)["metrics"] as? Map<String, Any?>
        return (metrics?.get("last_judged_at") as? String)?.takeIf { it.isNotEmpty() }
    }

    private fun sendHighMemory(server: String) {
        val newest = Instant.now().minusSeconds(1)
        for (ago in 9 downTo 0) {
            val body = """{"interval_ms": 30000, "host": {"cpu_pct": 5, "load1": 0.1, "mem_total": 1000, "mem_used": 600, "disk_total": 1000, "disk_used": 100,
                "net_rx_bps": 1, "net_tx_bps": 1, "io_read_bps": 0, "io_write_bps": 0}, "containers": [], "gpus": [], "missing": []}"""
            redis.opsForStream<String, String>().add(METRIC_STREAM, mapOf("kind" to "resource", "v" to "1", "server" to server,
                "at" to newest.minusSeconds(30L * ago).toEpochMilli().toString(), "body" to body))
        }
    }

    @Test
    fun `알림 서버가 한 건에 3초씩 걸리는 동안에도 판정은 제 주기에 계속 돈다`() {
        listOf("ci-agent-1", "ci-agent-2", "ci-agent-3", "ci-agent-4").forEach(::sendHighMemory)
        val deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos()
        while (inFlight.get() == 0 && System.nanoTime() < deadline) Thread.sleep(50)
        assertTrue(inFlight.get() > 0, "알림 서버가 보내기를 받지 않았다 — 넘음이 판정되지 않았거나 보내지 않는다")

        // 보내는 중인 동안(최대 약 10초 — 넘음 넷 × 3초) 판정이 제 주기에 도는지 본다. 판정 시각은 판정 바퀴가 직접 남긴 값(ThresholdRound.lastJudgedAt)을
        // 이 프로세스 안에서 50ms 마다 읽는다 — 헬스(HTTP)를 거치지 않아 헬스의 지연과 섞이지 않는다. 멈춤은 "읽은 순간 마지막 판정이 얼마나 오래됐나"로 본다.
        // 판정 간격은 200ms 이므로 2.5초(12배 넘게)는 넉넉하다 — 한 건 보내기(3초) 동안이라도 판정이 막히면 넘는다.
        val observations = mutableListOf<Duration>()
        val watchEnd = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        var lastSending = System.nanoTime()
        // 보내는 중 — 지금 붙잡힌 건이 있거나, 건과 건 사이의 짧은 틈(1초 미만)
        while (System.nanoTime() - lastSending < Duration.ofSeconds(1).toNanos() && System.nanoTime() < watchEnd) {
            if (inFlight.get() > 0) lastSending = System.nanoTime()
            thresholdRound.lastJudgedAt?.let { observations += Duration.between(it, Instant.now()) }
            Thread.sleep(50)
        }
        println("observe-test dispatch-schedule observations=${observations.size} stalest(ms)=${observations.maxOrNull()?.toMillis()} sent=${sentCount.get()}")
        assertTrue(observations.size >= 3, "보내는 동안 판정 시각을 ${observations.size}번밖에 읽지 못했다 — 보내는 중이 너무 짧았거나 판정이 한 번도 돌지 않았다(관찰 부족 — 판정 멈춤과 따로 본다)")
        val stalest = observations.max()
        assertTrue(stalest < Duration.ofMillis(2_500),
            "보내는 동안 판정이 멈췄다 — 물은 순간 마지막 판정이 ${stalest.toMillis()}ms 전(판정 간격 200ms, 관찰 ${observations.size}번)")
    }

    @AfterAll
    fun cleanUp() {
        alertServer.stop(0)
        redis.delete(METRIC_STREAM)
        redis.delete(LOG_STREAM)
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
        administrationPool.close()
    }

    companion object {
        private const val SCHEMA = "dispatch_schedule"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val METRIC_STREAM = "dispatch-schedule-metrics-$suffix"
        private val LOG_STREAM = "dispatch-schedule-logs-$suffix"
        private val administrationPool = EngineSupport.dataSource(size = 1)
        private val administration by lazy { JdbcTemplate(administrationPool) }
        private val json = JsonMapper.builder().build()
        private val inFlight = AtomicInteger()
        private val sentCount = AtomicInteger()

        /** 알림 서버 대역 — 한 건에 3초 걸려 202 */
        private val alertServer: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/events") { exchange ->
                inFlight.incrementAndGet()
                sentCount.incrementAndGet()
                try {
                    exchange.requestBody.readAllBytes()
                    Thread.sleep(3_000)
                    val answer = """{"success":true,"data":{"sent":true}}""".toByteArray()
                    exchange.sendResponseHeaders(202, answer.size.toLong())
                    exchange.responseBody.use { it.write(answer) }
                } finally {
                    inFlight.decrementAndGet()
                }
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
            registry.add("observe.group") { "dispatch-schedule-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.metrics.group") { "dispatch-schedule-$suffix" }
            registry.add("observe.metrics.judge-interval-ms") { "200" }
            registry.add("observe.alert.dispatch-interval-ms") { "200" }
            registry.add("observe.metrics.alert.memory-pct") { "50" }
            registry.add("observe.metrics.alert.memory-resolve-pct") { "40" }
            registry.add("observe.alert.events-url") { "http://127.0.0.1:${alertServer.address.port}/events" }
            registry.add("observe.alert.secret") { "alert-secret" }
            registry.add("observe.alert.timeout-ms") { "5000" }
            registry.add("observe.secret") { "dispatch-secret" }
        }
    }
}
