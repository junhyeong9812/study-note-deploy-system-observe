package xyz.junproject.observe.engine

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.env.Environment
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import tools.jackson.databind.json.JsonMapper
import xyz.junproject.observe.visit.CapturedLogs
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.SEARCH_WORDS
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import xyz.junproject.observe.visit.VisitRecords.VISITOR_TWO
import xyz.junproject.observe.visit.VisitSchedule
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 실제 앱(Spring 컨텍스트 전체 + 실제 PostgreSQL · Redis) — 방문 수집기가 켜진 상태에서:
 * `GET /visits/summary` 의 인증 · 422 · GET 만, 스트림 → 저장 → 요약의 한 바퀴, 헬스의 `visits` 칸,
 * 그리고 **주소 원문은 표에만 있다** — 수집 · 해석 실패 · 저장 실패 · 조회를 한 바퀴 돈 뒤 애플리케이션 로그 전체(뜰 때부터) ·
 * 모든 조회 응답의 본문 · 헬스 본문에 문서용 가짜 주소 · 구분값 · 검색어의 글자가 없다. 표는 전용 스키마(`visit_http`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext   // 클래스가 끝나면 문맥(연결 풀)을 닫는다 — 문맥이 쌓이면 테스트 DB 의 연결 상한(100)을 넘는다
class VisitHttpEngineTest {
    @Autowired private lateinit var environment: Environment
    @Autowired private lateinit var redis: StringRedisTemplate
    @Autowired private lateinit var visitSchedule: VisitSchedule

    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
    private val json = JsonMapper.builder().build()
    private val base get() = "http://127.0.0.1:${environment.getProperty("local.server.port")}"
    /** 이 테스트가 본 모든 응답 본문 — 끝에 주소 · 구분값 · 검색어가 없는지 한꺼번에 본다 */
    private val responses = mutableListOf<String>()

    private fun call(method: String, path: String, secret: String? = SECRET): HttpResponse<String> {
        val builder = HttpRequest.newBuilder(URI("$base$path")).timeout(Duration.ofSeconds(10))
        if (secret != null) builder.header("X-Observe-Secret", secret)
        builder.method(method, HttpRequest.BodyPublishers.noBody())
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString()).also { synchronized(responses) { responses += it.body() } }
    }

    @Suppress("UNCHECKED_CAST")
    private fun envelope(response: HttpResponse<String>): Map<String, Any?> = try {
        json.readValue(response.body(), Map::class.java) as Map<String, Any?>
    } catch (error: Exception) {
        fail("봉투가 JSON 이 아니다: status=${response.statusCode()} body=${response.body()}")
    }

    @Suppress("UNCHECKED_CAST")
    private fun summary(days: Int = 7): Map<String, Any?> {
        val response = call("GET", "/visits/summary?days=$days")
        assertEquals(200, response.statusCode(), response.body())
        return envelope(response)["data"] as Map<String, Any?>
    }

    @Suppress("UNCHECKED_CAST")
    private fun health(): Map<String, Any?> = envelope(call("GET", "/health", secret = null))

    @Suppress("UNCHECKED_CAST")
    private fun visitHealth(): Map<String, Any?> = health()["visits"] as? Map<String, Any?> ?: fail("헬스에 visits 칸이 없다: ${health()}")

    @Suppress("UNCHECKED_CAST")
    private fun totalViews(data: Map<String, Any?>, field: String) = (data["daily"] as List<Map<String, Any?>>).sumOf { (it[field] as Number).toLong() }

    private fun send(fields: Map<String, String>) = redis.opsForStream<String, String>().add(VISIT_STREAM, fields)!!.value

    private fun <T> eventually(timeout: Duration = Duration.ofSeconds(20), what: String, probe: () -> T?): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            runCatching(probe).getOrNull()?.let { return it }
            Thread.sleep(200)
        }
        fail<Unit>("$what — ${timeout.seconds}초 안에 되지 않았다")
        throw IllegalStateException()
    }

    @Test
    fun `요약 경로는 비밀값이 없거나 틀리면 401 이고 days 가 틀려도 401 이 먼저다`() {
        val unauthorized = mapOf("success" to false, "error" to mapOf("code" to "unauthorized"))
        for (path in listOf("/visits/summary", "/visits/summary?days=7", "/visits/summary?days=1")) {
            for (secret in listOf(null, "wrong")) {
                val response = call("GET", path, secret)
                assertEquals(401, response.statusCode(), "$path ${response.body()}")
                assertEquals(unauthorized, envelope(response))
            }
        }
    }

    @Test
    fun `days 가 허용 목록 밖이면 422 invalid_request 이고 GET 만 받는다`() {
        for (days in listOf("1", "14", "365", "x")) {
            val response = call("GET", "/visits/summary?days=$days")
            assertEquals(422, response.statusCode(), "$days ${response.body()}")
            @Suppress("UNCHECKED_CAST")
            assertEquals("invalid_request", (envelope(response)["error"] as Map<String, Any?>)["code"])
        }
        for (method in listOf("POST", "PUT", "DELETE")) assertEquals(405, call(method, "/visits/summary").statusCode(), method)
    }

    @Test
    fun `수집 · 해석 실패 · 저장 실패 · 조회를 한 바퀴 돌아도 주소 원문은 표에만 있다`() {
        assertNotNull(visitSchedule)
        eventually(what = "헬스의 visits.status 가 ok(표가 만들어짐)") { visitHealth().takeIf { it["status"] == "ok" } }
        val before = summary()

        // 수집: 사람 셋(데스크톱 둘 · 모바일 하나, 하나는 쿼리가 섞여 옴) + 봇 하나
        send(VisitRecords.fields(ip = ADDRESS_V4, visitor = VISITOR_ONE))
        send(VisitRecords.fields(ip = ADDRESS_V6, visitor = VISITOR_TWO, device = "mobile", referrerHost = null))
        send(VisitRecords.fields(ip = ADDRESS_V4, visitor = VISITOR_ONE, path = "/search?q=$SEARCH_WORDS", screen = "search"))
        send(VisitRecords.fields(ip = ADDRESS_V6, visitor = "b0b0b0b0" + "0".repeat(56), device = "bot"))
        // 해석 실패: 주소 꼴이 틀림(글자 안에 주소가 든다) · 제어 문자(검색어가 든다)
        val badAddress = send(VisitRecords.fields(ip = "$ADDRESS_V4:443"))
        val badControl = send(VisitRecords.fields(ip = ADDRESS_V6, path = "/search\u0000?q=$SEARCH_WORDS"))

        val collected = eventually(what = "넣은 넷이 요약에 나오기") {
            summary().takeIf { totalViews(it, "views_with_bots") - totalViews(before, "views_with_bots") == 4L }
        }
        assertEquals(3L, totalViews(collected, "views") - totalViews(before, "views"), "봇을 빼지 않았다")
        @Suppress("UNCHECKED_CAST")
        val paths = ((collected["top_paths"] as Map<String, Any?>)["items"] as List<Map<String, Any?>>).map { it["path"] }
        assertTrue("/search" in paths, "쿼리를 자른 경로가 없다: $paths")
        val dropped = eventually(what = "헬스의 visits.dropped 에 두 이유가 나오기") {
            visitHealth().takeIf {
                @Suppress("UNCHECKED_CAST") val reasons = it["dropped"] as Map<String, Any?>
                (reasons["invalid_ip"] as Number).toLong() >= 1 && (reasons["control_character"] as Number).toLong() >= 1
            }
        }
        assertTrue((dropped["query_stripped"] as Number).toLong() >= 1, "$dropped")
        assertEquals(0L, (dropped["unacknowledged"] as Number).toLong(), "버린 항목이 미확인으로 남았다: $dropped")

        // 저장 실패: 표에 넣을 때마다 실패하게 한다 — 드라이버의 묶음 오류 문구에는 넣은 값이 실린다
        administration.execute("CREATE FUNCTION $SCHEMA.refuse_visit() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RAISE EXCEPTION 'refused for test'; END \$\$")
        administration.execute("CREATE TRIGGER refuse_visit BEFORE INSERT ON $SCHEMA.visit_event FOR EACH ROW EXECUTE FUNCTION $SCHEMA.refuse_visit()")
        send(VisitRecords.fields(ip = ADDRESS_V4, visitor = VISITOR_ONE, path = "/wiki/during-failure"))
        val failing = eventually(what = "헬스의 visits.status 가 failing") { health().takeIf { (it["visits"] as? Map<*, *>)?.get("status") == "failing" } }
        assertEquals("ok", failing["status"], "방문 저장의 실패가 로그 수집의 상태를 바꿨다: $failing")
        @Suppress("UNCHECKED_CAST")
        assertTrue(((failing["visits"] as Map<String, Any?>)["unacknowledged"] as Number).toLong() >= 1, "저장에 실패한 항목을 확인해 버렸다: $failing")
        administration.execute("DROP TRIGGER refuse_visit ON $SCHEMA.visit_event")
        eventually(what = "막혔던 항목이 다시 받아져 요약에 나오기") {
            summary().takeIf { totalViews(it, "views_with_bots") - totalViews(before, "views_with_bots") == 5L }
        }
        eventually(what = "헬스의 visits.status 가 다시 ok") { visitHealth().takeIf { it["status"] == "ok" } }

        // 조회: 요약 셋 + 기존 조회
        for (days in listOf(7, 30, 90)) summary(days)
        for (path in listOf("/logs", "/logs/stats", "/metrics/latest", "/health")) call("GET", path, secret = if (path == "/health") null else SECRET)

        // 주소는 표에 실제로 있다(이 검사가 헛돌지 않는다는 짝)
        assertEquals(3L, administration.queryForObject("SELECT count(*) FROM $SCHEMA.visit_event WHERE ip = ?", Long::class.java, ADDRESS_V4))
        assertEquals(0L, administration.queryForObject("SELECT count(*) FROM $SCHEMA.visit_event WHERE path LIKE ?", Long::class.java, "%$SEARCH_WORDS%"))

        val logText = logs.text()
        assertTrue(logText.contains(badAddress) && logText.contains(badControl), "버린 항목이 로그에 드러나지 않는다 — 로그를 모으지 못했거나 조용히 버렸다")
        val bodies = synchronized(responses) { responses.joinToString("\n") }
        for (secret in listOf(ADDRESS_V4, ADDRESS_V6, VISITOR_ONE, VISITOR_TWO, SEARCH_WORDS)) {
            assertFalse(logText.contains(secret), "애플리케이션 로그에 $secret 이 나왔다:\n" + logText.lines().filter { it.contains(secret) }.take(5).joinToString("\n"))
            assertFalse(bodies.contains(secret), "응답 본문에 $secret 이 나왔다")
        }
    }

    /** 표에 줄 하나를 바로 넣는다(수집을 거치지 않고) — 가장 오래된 줄을 만들려고. 주소는 표에만 있는 값 */
    private fun insertDirectly(at: Instant, sequence: Int) = administration.update(
        """INSERT INTO $SCHEMA.visit_event (stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip)
           VALUES (?, ?, '/old', 'topic', NULL, 'desktop', ?, ?)""", at.toEpochMilli(), sequence, VISITOR_TWO, ADDRESS_V6)

    @Test
    fun `가장 오래된 줄이 90일과 26시간보다 오래되면 HTTP 헬스의 방문 상태가 retention_overdue 이고 25시간이면 ok 이며 표가 비면 ok 이다`() {
        eventually(what = "헬스의 visits.status 가 ok(표가 만들어짐)") { visitHealth().takeIf { it["status"] == "ok" } }
        val retention = Duration.ofDays(90)
        val overdueAt = Instant.now().minus(retention).minus(Duration.ofHours(26)).minus(Duration.ofMinutes(10)).truncatedTo(ChronoUnit.MILLIS)
        val withinAt = Instant.now().minus(retention).minus(Duration.ofHours(25)).truncatedTo(ChronoUnit.MILLIS)
        try {
            insertDirectly(overdueAt, 9_001)
            val overdue = eventually(what = "헬스의 visits.status 가 retention_overdue") { visitHealth().takeIf { it["status"] == "retention_overdue" } }
            assertEquals(overdueAt.toString(), overdue["oldest_stored_at"], "$overdue")
            val whole = call("GET", "/health", secret = null)
            assertEquals(200, whole.statusCode(), "밀린 방문 삭제가 헬스의 HTTP 판정을 바꿨다: ${whole.body()}")
            assertEquals("ok", envelope(whole)["status"], "밀린 방문 삭제가 로그 수집의 상태를 바꿨다: ${whole.body()}")

            administration.update("DELETE FROM $SCHEMA.visit_event WHERE stream_seq = 9001 AND path = '/old'")
            insertDirectly(withinAt, 9_002)
            val within = eventually(what = "헬스의 visits.oldest_stored_at 이 90일 + 25시간 된 줄") {
                visitHealth().takeIf { it["oldest_stored_at"] == withinAt.toString() }
            }
            assertEquals("ok", within["status"], "90일 + 25시간 된 줄을 밀렸다고 봤다: $within")

            administration.update("DELETE FROM $SCHEMA.visit_event")
            val empty = eventually(what = "헬스의 visits.oldest_stored_at 이 빈 글자") { visitHealth().takeIf { it["oldest_stored_at"] == "" } }
            assertEquals("ok", empty["status"], "빈 표를 밀렸다고 봤다: $empty")
        } finally {
            administration.update("DELETE FROM $SCHEMA.visit_event WHERE path = '/old' AND stream_seq IN (9001, 9002)")
        }
    }

    @AfterAll
    fun cleanUp() {
        logs.close()
        redis.delete(VISIT_STREAM)
        redis.delete(LOG_STREAM)
        redis.delete(METRIC_STREAM)
        administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
    }

    @Test
    fun `앱에서 방문 항목은 저장된 뒤 스트림에서 지워지고 같은 앱의 로그 스트림 항목은 남으며 스트림 정리가 배선돼 있다`() {
        val streams = redis.opsForStream<String, String>()
        val visitId = send(VisitRecords.fields(path = "/wiki/v2b-wiring"))
        val logId = streams.add(LOG_STREAM, mapOf("level" to "info", "line" to "req-v2b-wiring:backend:still here"))!!.value
        eventually(what = "방문 항목이 저장된 뒤 스트림에서 지워진다") {
            streams.range(VISIT_STREAM, Range.closed(visitId, visitId))!!.isEmpty().takeIf { it && (visitHealth()["stream_deleted"] as Number).toLong() >= 1 }
        }
        eventually(what = "로그 항목이 저장된다") {
            @Suppress("UNCHECKED_CAST")
            ((envelope(call("GET", "/logs?request_id=req-v2b-wiring"))["data"] as Map<String, Any?>)["logs"] as List<Any?>).singleOrNull()
        }
        assertEquals(listOf(logId), streams.range(LOG_STREAM, Range.closed(logId, logId))!!.map { it.id.value }, "로그 스트림의 항목까지 지웠다")
        assertNotNull(visitSchedule.trimOldVisitStream(Instant.now()), "하루 한 번의 정리에 스트림 지우개가 배선되지 않았다")
        assertNotEquals("", visitHealth()["last_stream_trim_at"])
    }

    companion object {
        private const val SCHEMA = "visit_http"
        private const val SECRET = "visit-secret"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val VISIT_STREAM = "visit-http-visits-$suffix"
        private val LOG_STREAM = "visit-http-logs-$suffix"
        private val METRIC_STREAM = "visit-http-metrics-$suffix"
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }
        /** 뜰 때부터의 애플리케이션 로그 전체 */
        private lateinit var logs: CapturedLogs

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            logs = CapturedLogs()
            administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
            administration.execute("CREATE SCHEMA $SCHEMA")
            registry.add("observe.db.url") { EngineSupport.databaseUrl + "?currentSchema=$SCHEMA,public" }
            registry.add("observe.db.user") { EngineSupport.databaseUser }
            registry.add("observe.db.password") { EngineSupport.databasePassword }
            registry.add("spring.data.redis.url") { requireNotNull(System.getenv("OBSERVE_TEST_REDIS_URL")) { "OBSERVE_TEST_REDIS_URL 이 없다" } }
            registry.add("observe.stream") { LOG_STREAM }
            registry.add("observe.group") { "visit-http-$suffix" }
            registry.add("observe.metrics.stream") { METRIC_STREAM }
            registry.add("observe.metrics.group") { "visit-http-$suffix" }
            registry.add("observe.visits.stream") { VISIT_STREAM }
            registry.add("observe.visits.group") { "visit-http-$suffix" }
            registry.add("observe.secret") { SECRET }
            registry.add("observe.visits.retention-cron") { "0 0 0 1 1 *" }   // 테스트 중에 하루 한 번의 삭제가 돌아 넣은 줄을 지우지 않게
        }
    }
}
