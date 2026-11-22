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
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 명세 ② 세부 3 — 관측 서버의 본문 오류: 인증(`X-Observe-Secret`)을 본문 해석보다 먼저 판정하고, 해석할 수 없는 본문은
 * 프레임워크의 400·415 가 아니라 422 `invalid_request` 봉투. 본문 해석은 Spring 단계라 컨트롤러 직접 호출로는 닿지 않으므로
 * 실제 앱을 띄워 HTTP 로 부른다. 두 POST 경로 모두, 기존 `/logs`·`/logs/stats`·`/health` 의 동작은 그대로.
 * 표는 전용 스키마(`auth_http`)에 만든다 — 다른 엔진 테스트와 섞이지 않게.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AuthAttemptHttpEngineTest {
    @Autowired private lateinit var environment: Environment

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"
    private val postPaths = listOf("/auth/attempts", "/auth/attempts/rejected")

    /** [contentType] 이 null 이면 Content-Type 머리글을 붙이지 않는다. [body] 가 null 이면 본문 없이 보낸다 */
    private fun call(method: String, path: String, body: String? = null, secret: String? = SECRET, contentType: String? = "application/json"): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("$base$path")).timeout(Duration.ofSeconds(10))
        if (secret != null) builder.header("X-Observe-Secret", secret)
        if (contentType != null) builder.header("Content-Type", contentType)
        builder.method(method, if (body == null) HttpRequest.BodyPublishers.noBody() else HttpRequest.BodyPublishers.ofString(body))
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    @Suppress("UNCHECKED_CAST")
    private fun envelope(response: HttpResponse<String>): Map<String, Any?> = try {
        json.readValue(response.body(), Map::class.java) as Map<String, Any?>
    } catch (error: Exception) {
        fail("봉투가 JSON 이 아니다: status=${response.statusCode()} body=${response.body()}")
    }

    private val unauthorizedEnvelope = mapOf("success" to false, "error" to mapOf("code" to "unauthorized"))

    private fun assertUnauthorized(response: HttpResponse<String>, label: String) {
        assertEquals(401, response.statusCode(), "$label: ${response.body()}")
        assertEquals(unauthorizedEnvelope, envelope(response), label)
    }

    private fun assertInvalidRequest(response: HttpResponse<String>, label: String) {
        assertEquals(422, response.statusCode(), "$label: ${response.body()}")
        val body = envelope(response)
        assertEquals(false, body["success"], label)
        @Suppress("UNCHECKED_CAST")
        assertEquals("invalid_request", (body["error"] as? Map<String, Any?>)?.get("code"), "$label: ${response.body()}")
    }

    private fun validBody(path: String): String {
        val id = UUID.randomUUID()
        val at = Instant.parse("2026-11-22T03:00:00Z")
        return if (path == "/auth/attempts")
            """{"attempt_id":"$id","at":"$at","credential_ok":false,"ip":"203.0.113.7","user_agent":"http/1"}"""
        else
            """{"attempt_id":"$id","at":"$at","reason":"malformed","ip":"203.0.113.7","user_agent":"http/1"}"""
    }

    /** 이 클래스가 만든 시도 기록 수 — 401·422 로 끝난 요청은 기록되지 않아야 한다 */
    private fun recordedAttempts(): Long =
        administration.queryForObject("SELECT count(*) FROM $SCHEMA.auth_attempt", Long::class.java)!!

    /** 해석할 수 없는 본문의 꼴들 — 이름과 (본문, Content-Type) */
    private val unreadableBodies = listOf(
        Triple("깨진 JSON `{`", "{", "application/json"),
        Triple("깨진 JSON 끝이 잘림", """{"attempt_id":"x",""", "application/json"),
        Triple("본문 없음(JSON 머리글)", null, "application/json"),
        Triple("본문 없음(머리글 없음)", null, null),
        Triple("빈 글자 본문", "", "application/json"),
        Triple("Content-Type 이 text/plain", "VALID", "text/plain"),
        Triple("Content-Type 이 form", "VALID", "application/x-www-form-urlencoded"),
        Triple("Content-Type 머리글 없음", "VALID", null),
        Triple("JSON 이지만 배열", "[]", "application/json"),
        Triple("JSON 이지만 글자", "\"x\"", "application/json"),
        Triple("JSON null", "null", "application/json"),
    )

    @Test
    fun `해석할 수 없는 본문에 비밀값이 없거나 틀리면 401 unauthorized 봉투 — 인증이 본문 해석보다 먼저다`() {
        val before = recordedAttempts()
        for (path in postPaths) {
            for ((label, body, contentType) in unreadableBodies) {
                val content = if (body == "VALID") validBody(path) else body
                assertUnauthorized(call("POST", path, content, secret = null, contentType = contentType), "$path $label 비밀값 없음")
                assertUnauthorized(call("POST", path, content, secret = "wrong", contentType = contentType), "$path $label 비밀값 틀림")
            }
        }
        assertEquals(before, recordedAttempts(), "인증에 실패한 요청이 기록됐다")
    }

    @Test
    fun `해석할 수 없는 본문에 맞는 비밀값이면 422 invalid_request 봉투이고 기록하지 않는다`() {
        val before = recordedAttempts()
        for (path in postPaths) {
            for ((label, body, contentType) in unreadableBodies) {
                val content = if (body == "VALID") validBody(path) else body
                assertInvalidRequest(call("POST", path, content, contentType = contentType), "$path $label")
            }
        }
        assertEquals(before, recordedAttempts(), "해석할 수 없는 본문이 기록됐다")
    }

    @Test
    fun `replayed false 에 outcome 칸이 null 로 있어도 HTTP 로 422`() {
        val id = UUID.randomUUID()
        val body = """{"attempt_id":"$id","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"http/1","replayed":false,"outcome":null}"""
        assertInvalidRequest(call("POST", "/auth/attempts", body), "replayed false + outcome null")
        val withoutReplayed = """{"attempt_id":"$id","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"http/1","outcome":null}"""
        assertInvalidRequest(call("POST", "/auth/attempts", withoutReplayed), "replayed 없음 + outcome null")
    }

    @Test
    fun `replayed 칸이 null 이면 HTTP 로 422 이고 기록하지 않는다`() {
        // 세부 4: replayed 칸이 있으면 Boolean 이어야 한다 — 칸이 없을 때만 기본 false
        val before = recordedAttempts()
        val withNull = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"http/1","replayed":null}"""
        assertInvalidRequest(call("POST", "/auth/attempts", withNull), "replayed null")
        val withNullAndOutcome = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","credential_ok":false,"ip":"203.0.113.7","user_agent":"http/1","replayed":null,"outcome":"failure"}"""
        assertInvalidRequest(call("POST", "/auth/attempts", withNullAndOutcome), "replayed null + outcome")
        assertEquals(before, recordedAttempts(), "422 로 거절한 요청이 기록됐다")
    }

    @Test
    fun `거절 기록 경로는 계약에 없는 replayed 칸을 무시하고 기록한다`() {
        val before = recordedAttempts()
        for (extra in listOf("\"replayed\":true", "\"replayed\":null", "\"outcome\":\"success\"")) {
            val body = """{"attempt_id":"${UUID.randomUUID()}","at":"2026-11-22T03:00:00Z","reason":"rate_limited","ip":"203.0.113.7","user_agent":"http/1",$extra}"""
            val response = call("POST", "/auth/attempts/rejected", body)
            assertEquals(200, response.statusCode(), "$extra ${response.body()}")
        }
        assertEquals(before + 3, recordedAttempts())
    }

    @Test
    fun `맞는 본문과 맞는 비밀값이면 두 POST 경로가 200 — 본문 해석 규칙이 정상 요청을 막지 않는다`() {
        for (path in postPaths) {
            val response = call("POST", path, validBody(path))
            assertEquals(200, response.statusCode(), "$path ${response.body()}")
            assertEquals(true, envelope(response)["success"])
        }
    }

    @Test
    fun `기존 경로의 동작은 그대로다`() {
        assertUnauthorized(call("GET", "/logs", secret = null, contentType = null), "/logs 비밀값 없음")
        assertUnauthorized(call("GET", "/logs/stats", secret = "wrong", contentType = null), "/logs/stats 비밀값 틀림")
        assertEquals(200, call("GET", "/logs", contentType = null).statusCode())
        assertEquals(200, call("GET", "/logs/stats", contentType = null).statusCode())
        val badLimit = call("GET", "/logs?limit=many", contentType = null)
        assertEquals(422, badLimit.statusCode(), badLimit.body())
        @Suppress("UNCHECKED_CAST")
        assertEquals("invalid_request", (envelope(badLimit)["error"] as Map<String, Any?>)["code"])
        val health = call("GET", "/health", secret = null, contentType = null)
        assertTrue(health.statusCode() in setOf(200, 503), health.body())
        assertTrue(envelope(health).containsKey("status"), health.body())
    }

    @AfterAll
    fun dropSchema() {
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    companion object {
        private const val SCHEMA = "auth_http"
        private const val SECRET = "http-secret"
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
            administration.execute("CREATE SCHEMA $SCHEMA")
            val suffix = UUID.randomUUID().toString().take(8)
            registry.add("observe.db.url") { EngineSupport.databaseUrl + "?currentSchema=$SCHEMA,public" }
            registry.add("observe.db.user") { EngineSupport.databaseUser }
            registry.add("observe.db.password") { EngineSupport.databasePassword }
            registry.add("spring.data.redis.url") { requireNotNull(System.getenv("OBSERVE_TEST_REDIS_URL")) { "OBSERVE_TEST_REDIS_URL 이 없다" } }
            registry.add("observe.stream") { "auth-http-$suffix" }
            registry.add("observe.group") { "auth-http-$suffix" }
            registry.add("observe.secret") { SECRET }
        }
    }
}
