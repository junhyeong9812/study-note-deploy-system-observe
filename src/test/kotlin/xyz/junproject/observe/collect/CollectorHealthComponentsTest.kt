package xyz.junproject.observe.collect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.BadSqlGrammarException
import org.springframework.jdbc.CannotGetJdbcConnectionException
import org.springframework.data.redis.RedisConnectionFailureException
import tools.jackson.databind.json.JsonMapper
import java.sql.SQLException
import java.time.Duration
import java.time.Instant

/**
 * ⑬ T9c: `GET /health` 에 의존 칸 `components: {"redis": {…}, "postgres": {…}}` 를 더한다 — 칸을 더하는 것이라
 * 지금의 다섯 상태 낱말과 그 판정 · HTTP 상태(200 은 `ok` 하나) · 평평한 칸 · `failure` 글은 그대로다.
 *
 * 칸마다 `{"status": "UP"|"DOWN", "took_ms": <정수>|null, "detail"?: …}`:
 * - `took_ms` 는 그 의존을 보는 **지금 있는 확인 한 번**의 걸린 시간이다 — Redis 는 미확인 건수(`unacknowledged`), DB 는 가장 최근 행의
 *   시각(`latestStoredAt`). 단조 시계 · 정수 ms 내림. 확인을 한 번 더 부르지 않는다.
 * - 확인이 예외로 끝나면 DOWN · `took_ms` 는 null(규약 4.3 "응답을 받지 못하면 null") · `detail` 은 예외 종류 이름만(지금 `failure` 와 같은 꼴).
 *   지금의 확인은 값을 돌려주거나 예외를 던질 뿐이라 "응답을 받지 못함"과 "받았으나 실패"를 예외 종류로 가를 수 없다 — 예외로 끝난 확인은 모두 null 이다.
 * - 표가 없다는 답(SQLSTATE 42P01)은 지금처럼 DB 에 닿은 것이다 — UP · `took_ms` 정수.
 * - UP 에는 `detail` 이 없다. `detail` 에 예외의 글 · 주소 · 비밀값을 싣지 않는다.
 */
class CollectorHealthComponentsTest {
    private val stream = FakeLogStream()
    private val sink = FakeLogSink()
    private val startedAt = Instant.parse("2026-11-21T00:00:00Z")
    private var now = startedAt
    private var redisDelayMs = 0L
    private var databaseDelayMs = 0L
    private var redisFailure: Throwable? = null
    private var databaseFailure: Throwable? = null
    private var tableMissing = false
    private var unacknowledgedCalls = 0
    private var latestStoredAtCalls = 0
    private val collector = LogCollector(stream, sink, block = Duration.ZERO, clock = { now })
    private val health = CollectorHealth(
        collector,
        unacknowledged = {
            unacknowledgedCalls++
            Thread.sleep(redisDelayMs)
            redisFailure?.let { throw it }
            stream.unacknowledgedCount()
        },
        lag = { 0L },
        latestStoredAt = {
            latestStoredAtCalls++
            Thread.sleep(databaseDelayMs)
            databaseFailure?.let { throw it }
            if (tableMissing) throw BadSqlGrammarException("latest", "SELECT stream_ms FROM log_entry",
                SQLException("ERROR: relation \"log_entry\" does not exist", "42P01"))
            null
        },
        lastRetention = { null },
        startedAt = startedAt,
    )

    private fun check() = health.report(now).let { (if (it.healthy) 200 else 503) to it.body }

    @Suppress("UNCHECKED_CAST")
    private fun components(body: Map<String, Any>): Map<String, Map<String, Any?>> {
        val components = body["components"] as? Map<String, Map<String, Any?>> ?: fail("components 칸이 객체가 아니다: $body")
        assertEquals(setOf("redis", "postgres"), components.keys, "의존 이름은 redis · postgres 둘뿐이다(database 가 아니다)")
        return components
    }

    /** 정수(Int · Long — 소수가 아니다)이고 0 이상 */
    private fun tookMs(component: Map<String, Any?>): Long {
        val tookMs = component["took_ms"]
        assertTrue(tookMs is Long || tookMs is Int, "took_ms 가 정수가 아니다: $tookMs (${tookMs?.javaClass})")
        val value = (tookMs as Number).toLong()
        assertTrue(value >= 0, "took_ms 가 음수다: $value")
        return value
    }

    private fun assertUp(component: Map<String, Any?>): Long {
        assertEquals(setOf("status", "took_ms"), component.keys, "UP 에는 detail 이 없다: $component")
        assertEquals("UP", component["status"])
        return tookMs(component)
    }

    private fun assertDown(component: Map<String, Any?>, exceptionType: String) {
        assertEquals(mapOf("status" to "DOWN", "took_ms" to null, "detail" to exceptionType), component)
    }

    private fun assertNearDelay(tookMs: Long) = assertTrue(tookMs in DELAY_LOWER_MS..DELAY_UPPER_MS, "늦춘 확인의 took_ms 가 그 지연 근처가 아니다: $tookMs")
    private fun assertNotDelayed(tookMs: Long) = assertTrue(tookMs < DELAY_LOWER_MS, "늦추지 않은 확인의 took_ms 에 다른 확인의 시간이 섞였다: $tookMs")

    // --- 모양 ------------------------------------------------------------------------------------

    @Test
    fun `정상이면 200 ok 이고 redis 와 postgres 둘 다 UP 이며 took_ms 는 정수다`() {
        collector.runOnce()
        val (status, body) = check()
        assertEquals(200 to "ok", status to body["status"])
        val components = components(body)
        assertUp(components.getValue("redis"))
        assertUp(components.getValue("postgres"))
    }

    @Test
    fun `components 는 칸 하나를 더한 것이고 지금의 평평한 칸은 그대로 있다`() {
        collector.runOnce()
        val (_, body) = check()
        assertEquals(EXISTING_FIELDS + "components", body.keys)
        assertEquals("", body["failure"])
        assertEquals(0L, body["unacknowledged"])
        assertEquals(0L, body["lag"])
        assertEquals("", body["last_stored_at"])
    }

    @Test
    fun `예외로 끝난 확인의 took_ms 는 생략하지 않고 null 로 직렬화된다`() {
        collector.runOnce()
        databaseFailure = CannotGetJdbcConnectionException("닿지 못함")
        val serialized = JsonMapper.builder().build().writeValueAsString(check().second)
        assertTrue(serialized.contains("\"took_ms\":null"), serialized)
    }

    // --- 걸린 시간은 그 확인 하나의 시간 -------------------------------------------------------------------

    @Test
    fun `Redis 확인만 늦추면 redis 의 took_ms 만 그 지연 근처이고 postgres 는 아니다`() {
        collector.runOnce()
        redisDelayMs = DELAY_MS
        val (status, body) = check()
        assertEquals(200 to "ok", status to body["status"])
        val components = components(body)
        assertNearDelay(assertUp(components.getValue("redis")))
        assertNotDelayed(assertUp(components.getValue("postgres")))
    }

    @Test
    fun `DB 확인만 늦추면 postgres 의 took_ms 만 그 지연 근처이고 redis 는 아니다`() {
        collector.runOnce()
        databaseDelayMs = DELAY_MS
        val (status, body) = check()
        assertEquals(200 to "ok", status to body["status"])
        val components = components(body)
        assertNearDelay(assertUp(components.getValue("postgres")))
        assertNotDelayed(assertUp(components.getValue("redis")))
    }

    @Test
    fun `헬스 한 번에 두 확인은 한 번씩만 부른다`() {
        collector.runOnce()
        check()
        assertEquals(1 to 1, unacknowledgedCalls to latestStoredAtCalls, "정상")
        redisFailure = RedisConnectionFailureException("닿지 못함")
        databaseFailure = CannotGetJdbcConnectionException("닿지 못함")
        check()
        assertEquals(2 to 2, unacknowledgedCalls to latestStoredAtCalls, "둘 다 실패")
        redisFailure = null
        databaseFailure = null
        tableMissing = true
        check()
        assertEquals(3 to 3, unacknowledgedCalls to latestStoredAtCalls, "표가 없다는 답")
    }

    // --- DOWN ---------------------------------------------------------------------------------------

    @Test
    fun `Redis 만 닿지 못하면 503 unreachable 이고 redis 는 DOWN 에 took_ms null 이며 postgres 는 UP 이다`() {
        collector.runOnce()
        redisDelayMs = DELAY_MS                                         // 시간이 걸린 뒤 실패해도 null
        redisFailure = RedisConnectionFailureException("Unable to connect to redis:6379 $LEAK_MARK")
        val (status, body) = check()
        assertEquals(503 to "unreachable", status to body["status"])
        assertEquals("redis RedisConnectionFailureException", body["failure"])
        assertEquals(-1L, body["unacknowledged"])
        val components = components(body)
        assertDown(components.getValue("redis"), "RedisConnectionFailureException")
        assertUp(components.getValue("postgres"))
        assertFalse(body.toString().contains(LEAK_MARK), "예외의 글이 본문에 샜다: $body")
    }

    @Test
    fun `DB 만 닿지 못하면 503 unreachable 이고 postgres 는 DOWN 에 took_ms null 이며 redis 는 UP 이다`() {
        collector.runOnce()
        databaseDelayMs = DELAY_MS
        databaseFailure = CannotGetJdbcConnectionException("jdbc:postgresql://postgres:5432/observe $LEAK_MARK")
        val (status, body) = check()
        assertEquals(503 to "unreachable", status to body["status"])
        assertEquals("database CannotGetJdbcConnectionException", body["failure"])
        val components = components(body)
        assertDown(components.getValue("postgres"), "CannotGetJdbcConnectionException")
        assertUp(components.getValue("redis"))
        assertFalse(body.toString().contains(LEAK_MARK), "예외의 글이 본문에 샜다: $body")
    }

    @Test
    fun `둘 다 닿지 못하면 503 unreachable 이고 둘 다 DOWN 이며 failure 글은 지금과 같다`() {
        collector.runOnce()
        redisFailure = IllegalStateException("Redis 에 닿지 못함 $LEAK_MARK")
        databaseFailure = CannotGetJdbcConnectionException("DB 에 닿지 못함 $LEAK_MARK")
        val (status, body) = check()
        assertEquals(503 to "unreachable", status to body["status"])
        assertEquals("redis IllegalStateException; database CannotGetJdbcConnectionException", body["failure"])
        val components = components(body)
        assertDown(components.getValue("redis"), "IllegalStateException")
        assertDown(components.getValue("postgres"), "CannotGetJdbcConnectionException")
        assertFalse(body.toString().contains(LEAK_MARK), "예외의 글이 본문에 샜다: $body")
    }

    @Test
    fun `표가 없다는 답은 DB 에 닿은 것이라 postgres 는 UP 이고 took_ms 는 그 확인의 시간이다`() {
        tableMissing = true
        databaseDelayMs = DELAY_MS
        stream.ensureGroup()                                            // 그룹은 생겼고 스키마는 아직 — 지금은 starting
        val (status, body) = check()
        assertEquals(503 to "starting", status to body["status"])
        assertEquals("", body["failure"])
        val components = components(body)
        assertNearDelay(assertUp(components.getValue("postgres")))
        assertUp(components.getValue("redis"))
    }

    @Test
    fun `밀린 건수를 읽지 못해도 의존 칸에 넣지 않는다`() {
        val withFailingLag = CollectorHealth(collector, { stream.unacknowledgedCount() }, { throw IllegalStateException("ERR unknown subcommand") },
            { null }, { null }, startedAt = startedAt)
        collector.runOnce()
        val report = withFailingLag.report(now)
        assertTrue(report.healthy)
        val components = components(report.body)
        assertUp(components.getValue("redis"))
        assertUp(components.getValue("postgres"))
    }

    // --- ok 가 아닌 상태에도 있다 -----------------------------------------------------------------------

    @Test
    fun `starting 에도 components 가 있고 둘 다 UP 이어도 503 이다`() {
        stream.ensureGroup()
        val (status, body) = check()
        assertEquals(503 to "starting", status to body["status"])
        val components = components(body)
        assertUp(components.getValue("redis"))
        assertUp(components.getValue("postgres"))
    }

    @Test
    fun `stalled 에도 components 가 있고 둘 다 UP 이어도 503 이다`() {
        collector.runOnce()
        now = now.plusSeconds(120)
        val (status, body) = check()
        assertEquals(503 to "stalled", status to body["status"])
        val components = components(body)
        assertUp(components.getValue("redis"))
        assertUp(components.getValue("postgres"))
    }

    @Test
    fun `failing 에도 components 가 있고 둘 다 UP 이어도 503 이며 failure 는 바퀴의 사유 그대로다`() {
        stream.add(StreamRecord("1-0", "info", "a"))
        sink.failing = true
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503 to "failing", status to body["status"])
        assertEquals("IllegalStateException", body["failure"])
        val components = components(body)
        assertUp(components.getValue("redis"))
        assertUp(components.getValue("postgres"))
    }

    companion object {
        private const val DELAY_MS = 150L
        private const val DELAY_LOWER_MS = 140L       // 단조 시계로 잰 150ms 지연 — 느슨한 아래 경계
        private const val DELAY_UPPER_MS = 1_500L     // 느슨한 위 경계(느린 CI 를 견딘다)
        private const val LEAK_MARK = "SECRET-192.0.2.7"
        private val EXISTING_FIELDS = setOf(
            "status", "last_loop_at", "last_stored_at", "stored_since_start", "unacknowledged", "lag", "failure",
            "last_retention_at", "last_retention_deleted", "last_retention_failure",
            "last_auth_retention_at", "last_auth_retention_deleted", "last_auth_retention_failure",
        )
    }
}
