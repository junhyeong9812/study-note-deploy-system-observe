package xyz.junproject.observe.engine

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.env.Environment
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.json.JsonMapper
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.auth.AuthAttemptPolicy
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 앱이 실제로 뜨는가 — 실제 PostgreSQL·Redis 에 대고 Spring 컨텍스트 전체를 올려 새 경로 넷을 HTTP 로 한 번씩 부른다.
 * 설정 키(`observe.auth.*`)가 정책에 닿는지는 실패 한도를 2·잠금 1분으로 바꿔 본다(보관 기간은 기본 90일 그대로).
 * 표는 전용 스키마(`auth_smoke`)에 만든다 — 기본 스키마의 표를 보는 다른 엔진 테스트와 섞이지 않게.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApplicationSmokeEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var schedule: ObserveSchedule
    @Autowired private lateinit var policy: AuthAttemptPolicy

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"

    private fun call(method: String, path: String, body: String? = null, secret: String? = SECRET): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("$base$path")).timeout(Duration.ofSeconds(10))
        if (secret != null) builder.header("X-Observe-Secret", secret)
        if (body != null) builder.header("Content-Type", "application/json")
        builder.method(method, if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(body))
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    @Suppress("UNCHECKED_CAST")
    private fun data(response: HttpResponse<String>) = (json.readValue(response.body(), Map::class.java) as Map<String, Any?>)["data"] as Map<String, Any?>

    private fun attemptBody(id: UUID, at: Instant, credentialOk: Boolean) =
        """{"attempt_id":"$id","at":"$at","credential_ok":$credentialOk,"ip":"203.0.113.7","user_agent":"smoke/1"}"""

    @Test
    fun `새 경로 넷이 실제 앱에서 답하고 설정 키가 정책에 닿으며 null 은 JSON null 로 나간다`() {
        val at = Instant.now().truncatedTo(ChronoUnit.MILLIS)

        // 인증 없이는 401
        assertEquals(401, call("GET", "/auth/lock", secret = null).statusCode())

        // POST /auth/attempts — 첫 실패는 잠금 없음, locked_until 은 JSON null(빠지지 않는다)
        val first = call("POST", "/auth/attempts", attemptBody(UUID.randomUUID(), at, false))
        assertEquals(200, first.statusCode(), first.body())
        assertTrue(first.body().contains("\"locked_until\":null"), first.body())
        assertEquals(mapOf("allowed" to false, "outcome" to "failure", "locked_until" to null, "lock_started" to false, "failures" to 1), data(first))

        // 두 번째 실패에서 잠금 — 설정한 실패 한도 2·잠금 1분
        val secondId = UUID.randomUUID()
        val second = call("POST", "/auth/attempts", attemptBody(secondId, at.plusMillis(1), false))
        assertEquals(mapOf("allowed" to false, "outcome" to "failure", "locked_until" to at.plusMillis(1).plus(Duration.ofMinutes(1)).toString(),
            "lock_started" to true, "failures" to 2), data(second))
        // 같은 ID 의 재전송은 처음 판정 그대로
        assertEquals(data(second), data(call("POST", "/auth/attempts", attemptBody(secondId, at.plusMillis(1), false))))

        // POST /auth/attempts/rejected
        val rejected = call("POST", "/auth/attempts/rejected",
            """{"attempt_id":"${UUID.randomUUID()}","at":"${at.plusMillis(2)}","reason":"rate_limited","ip":"198.51.100.9","user_agent":"curl/8"}""")
        assertEquals(200, rejected.statusCode(), rejected.body())

        // GET /auth/attempts — 기간을 주지 않아도 방금 것 셋이 최신순
        val list = call("GET", "/auth/attempts")
        assertEquals(200, list.statusCode(), list.body())
        @Suppress("UNCHECKED_CAST")
        val rows = data(list)["attempts"] as List<Map<String, Any?>>
        assertEquals(listOf("rate_limited", "failure", "failure"), rows.map { it["outcome"] })
        assertEquals("", data(list)["next_before"])

        // GET /auth/lock
        val lock = call("GET", "/auth/lock")
        assertEquals(mapOf("locked" to true, "locked_until" to at.plusMillis(1).plus(Duration.ofMinutes(1)).toString(), "failures" to 2), data(lock))

        // 기존 경로가 그대로 답한다
        assertEquals(200, call("GET", "/logs").statusCode())
        assertEquals(200, call("GET", "/logs/stats").statusCode())
        val health = call("GET", "/health", secret = null)
        assertTrue(health.statusCode() in setOf(200, 503), health.body())
        assertTrue(health.body().contains("\"last_auth_retention_at\""), health.body())

        // 표는 전용 스키마에만 생겼다
        val tables = administration.queryForList(
            "SELECT table_schema FROM information_schema.tables WHERE table_name IN ('auth_attempt', 'auth_lock_state') AND table_schema IN ('public', '$SCHEMA')", String::class.java)
        assertEquals(listOf(SCHEMA, SCHEMA), tables)
    }

    @Test
    fun `예약 실행에 시도 기록 저장소가 배선되어 있고 보관 기간은 기본 90일이다`() {
        assertEquals(Duration.ofDays(90), policy.retention)
        assertEquals(2, policy.failureLimit)
        assertEquals(Duration.ofMinutes(1), policy.lockDuration)
        assertNotNull(schedule.deleteOldAuthAttempts(Instant.now()), "예약 실행이 시도 기록 저장소 없이 떴다")
        assertNull(schedule.lastAuthRetention?.failure)
    }

    @Test
    fun `실제 서버에서 본문 상한을 넘는 POST 는 422 봉투이고 상한 안의 정상 본문은 지난다`() {
        val oversized = """{"attempt_id":"${UUID.randomUUID()}","at":"${Instant.now()}","credential_ok":false,"ip":"203.0.113.7","user_agent":"smoke/1","padding":"${"a".repeat(64 * 1024)}"}"""
        for (path in listOf("/auth/attempts", "/auth/attempts/rejected")) {
            val response = call("POST", path, oversized)
            assertEquals(422, response.statusCode(), response.body())
            @Suppress("UNCHECKED_CAST")
            assertEquals("invalid_request", ((json.readValue(response.body(), Map::class.java) as Map<String, Any?>)["error"] as Map<String, Any?>)["code"])
        }
        // 시각을 기본 목록 기간(90일) 밖에 두어 다른 사례의 목록 단언에 섞이지 않게
        val padded = """{"attempt_id":"${UUID.randomUUID()}","at":"2000-01-01T00:00:00Z","reason":"malformed","ip":"203.0.113.7","user_agent":"smoke/1"}""".padEnd(8 * 1024)
        assertEquals(200, call("POST", "/auth/attempts/rejected", padded).statusCode())
    }

    @AfterAll
    fun dropSchema() {
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    companion object {
        private const val SCHEMA = "auth_smoke"
        private const val SECRET = "smoke-secret"
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            // 컨텍스트가 뜨기 전에 빈 전용 스키마를 만든다 — 검색 경로에 public 을 뒤에 두어 pg_trgm 을 찾게
            administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
            administration.execute("CREATE SCHEMA $SCHEMA")
            val suffix = UUID.randomUUID().toString().take(8)
            registry.add("observe.db.url") { EngineSupport.databaseUrl + "?currentSchema=$SCHEMA,public" }
            registry.add("observe.db.user") { EngineSupport.databaseUser }
            registry.add("observe.db.password") { EngineSupport.databasePassword }
            registry.add("spring.data.redis.url") { requireNotNull(System.getenv("OBSERVE_TEST_REDIS_URL")) { "OBSERVE_TEST_REDIS_URL 이 없다" } }
            registry.add("observe.stream") { "auth-smoke-$suffix" }
            registry.add("observe.group") { "auth-smoke-$suffix" }
            registry.add("observe.secret") { SECRET }
            registry.add("observe.auth.failure-limit") { "2" }
            registry.add("observe.auth.lock-duration") { "1m" }
        }
    }
}
