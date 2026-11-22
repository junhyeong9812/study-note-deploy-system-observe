package xyz.junproject.observe.auth

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.AbstractDataSource
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.RetentionResult
import java.sql.Connection
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 배선 — 정책 값의 설정, 스키마를 처음 닿을 때 만들기, 90일 삭제를 기존 예약 실행에 더하기, 헬스 본문, DB 불통의 503.
 * 실제 DB 없이 본다(실제 Spring 컨텍스트·DB 는 엔진 스모크 `ApplicationSmokeEngineTest`).
 */
class AuthAttemptWiringTest {
    private val now = Instant.parse("2026-11-22T03:40:00Z")

    /** 연결을 줄 수 없는 DB — 뜰 때 DB 가 없는 상황 */
    private class UnreachableDataSource : AbstractDataSource() {
        override fun getConnection(): Connection = throw SQLException("Connection refused — 내부 문구")
        override fun getConnection(username: String?, password: String?): Connection = getConnection()
    }

    /** 호출을 세고, 스키마 생성·삭제를 정해 둔 만큼 실패시키는 저장소 */
    private class CountingAuthAttempts : AuthAttempts {
        var schemaFailuresLeft = 0
        var schemaCalls = 0
        var deleteFailure: RuntimeException? = null
        val cutoffs = mutableListOf<Instant>()
        val calls = mutableListOf<String>()

        override fun createSchema() { schemaCalls++; if (schemaFailuresLeft-- > 0) throw IllegalStateException("DB 아직 없음") }
        override fun decide(request: AuthAttemptRequest): AuthVerdict { calls += "decide"; return AuthVerdict(true, AuthOutcome.SUCCESS, null, false, 0) }
        override fun recordRejected(attempt: AuthRejectedAttempt) { calls += "rejected" }
        override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> { calls += "list"; return emptyList() }
        override fun lockStatus(now: Instant): AuthLockStatus { calls += "lock"; return AuthLockStatus(false, null, 0) }
        override fun deleteOlderThan(cutoff: Instant): Int { deleteFailure?.let { throw it }; calls += "delete"; cutoffs += cutoff; return 3 }
    }

    private val request = AuthAttemptRequest(UUID.randomUUID(), now, false, "203.0.113.7", "Mozilla/5.0")

    // ── 정책 값의 설정 ────────────────────────────────────

    @Test
    fun `설정 키의 기본값 글자가 명세 수치 그대로의 정책이 된다`() {
        assertEquals(AuthAttemptPolicy(), ObserveConfiguration().authAttemptPolicy(5, "15m", "90d"))
        assertEquals(AuthAttemptPolicy(3, Duration.ofMinutes(1), Duration.ofDays(30)), ObserveConfiguration().authAttemptPolicy(3, "1m", "30d"))
    }

    @Test
    fun `정책 값이 말이 안 되면 뜰 때 실패한다`() {
        assertThrows(IllegalArgumentException::class.java) { AuthAttemptPolicy(failureLimit = 0) }
        assertThrows(IllegalArgumentException::class.java) { AuthAttemptPolicy(lockDuration = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { AuthAttemptPolicy(retention = Duration.ofDays(-1)) }
    }

    // ── 스키마는 처음 닿을 때 ──────────────────────────────

    @Test
    fun `스키마 생성이 실패하면 그 호출이 예외로 끝나고 다음 호출이 다시 만들며 성공한 뒤로는 묻지 않는다`() {
        val inner = CountingAuthAttempts().apply { schemaFailuresLeft = 2 }
        val attempts = SchemaOnFirstUseAuthAttempts(inner)
        assertThrows(IllegalStateException::class.java) { attempts.decide(request) }
        assertThrows(IllegalStateException::class.java) { attempts.lockStatus(now) }
        assertTrue(inner.calls.isEmpty(), "스키마가 없는데 저장소를 불렀다")

        attempts.decide(request)
        attempts.recordRejected(AuthRejectedAttempt(UUID.randomUUID(), now, "rate_limited", "203.0.113.7", "curl/8"))
        attempts.list(AuthAttemptQuery(now.minusSeconds(60), now))
        attempts.lockStatus(now)
        attempts.deleteOlderThan(now)
        assertEquals(3, inner.schemaCalls, "실패 두 번 뒤 한 번 만들고 그 뒤로는 다시 만들지 않는다")
        assertEquals(listOf("decide", "rejected", "list", "lock", "delete"), inner.calls)
    }

    @Test
    fun `동시에 처음 닿아도 스키마 생성은 한 번`() {
        val inner = CountingAuthAttempts()
        val attempts = SchemaOnFirstUseAuthAttempts(inner)
        (0 until 8).map { Thread { attempts.lockStatus(now) }.apply { start() } }.forEach { it.join(5_000) }
        assertEquals(1, inner.schemaCalls)
    }

    // ── DB 에 닿지 못할 때의 경로 ─────────────────────────

    @Test
    fun `DB 에 닿지 못하면 실제 저장소를 거친 경로 넷이 모두 503 이고 예외 문구를 내지 않는다`() {
        val store = AuthAttemptStore(JdbcTemplate(UnreachableDataSource()))
        val secret = MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }
        val body = mapOf("attempt_id" to UUID.randomUUID().toString(), "at" to now.toString(), "credential_ok" to false,
            "ip" to "203.0.113.7", "user_agent" to "Mozilla/5.0")
        // 판정은 트랜잭션을 여는 데서 실패한다(CannotCreateTransactionException — DataAccessException 이 아니다)
        for (attempts in listOf<AuthAttempts>(store, SchemaOnFirstUseAuthAttempts(store))) {
            val controller = AuthAttemptController(attempts, "secret")
            val responses = listOf(
                controller.decide(secret, body),
                controller.rejected(secret, body - "credential_ok" + ("reason" to "rate_limited")),
                controller.list(secret, null, null, null, null, now),
                controller.lock(secret, now),
            )
            for (response in responses) {
                assertEquals(503, response.statusCode.value(), "${response.body}")
                assertEquals(mapOf("success" to false, "error" to mapOf("code" to "query_failed")), response.body)
            }
        }
    }

    // ── 90일 삭제를 기존 예약 실행에 ───────────────────────

    private val unreachableLogStore = JdbcTemplate(UnreachableDataSource()).let { LogStore(it, it, it) }

    @Test
    fun `예약 실행 한 번이 30일 로그 삭제와 90일 시도 기록 삭제를 함께 돌고 기준은 정책의 보관 기간이다`() {
        val attempts = CountingAuthAttempts()
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, attempts, AuthAttemptPolicy(retention = Duration.ofDays(90)))
        assertNull(schedule.lastAuthRetention)
        assertEquals(3, schedule.deleteOldAuthAttempts(now))
        assertEquals(listOf(now.minus(Duration.ofDays(90))), attempts.cutoffs)
        assertEquals(RetentionResult(now, 3, null), schedule.lastAuthRetention)
    }

    @Test
    fun `로그 삭제가 실패해도 시도 기록 삭제는 돌고 각자의 결과가 남는다`() {
        val attempts = CountingAuthAttempts()
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, attempts)
        schedule.deleteOldLogs()   // 예약이 부르는 그 함수
        assertEquals("CannotGetJdbcConnectionException", schedule.lastRetention?.failure)
        assertNull(schedule.lastRetention?.deleted)
        assertEquals(listOf("delete"), attempts.calls, "로그 삭제 실패가 시도 기록 삭제를 막았다")
        assertEquals(3, schedule.lastAuthRetention?.deleted)
        assertNull(schedule.lastAuthRetention?.failure)
    }

    @Test
    fun `시도 기록 삭제가 실패해도 예약 실행은 예외 없이 끝나고 실패 종류가 남는다`() {
        val attempts = CountingAuthAttempts().apply { deleteFailure = org.springframework.dao.QueryTimeoutException("내부 문구") }
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, attempts)
        schedule.deleteOldLogs()
        assertNotNull(schedule.lastRetention, "로그 삭제가 돌지 않았다")
        assertEquals(RetentionResult(schedule.lastAuthRetention!!.ranAt, null, "QueryTimeoutException"), schedule.lastAuthRetention)
    }

    @Test
    fun `헬스 본문에 시도 기록 삭제의 결과가 나오고 실패해도 판정에는 넣지 않는다`() {
        val collector = LogCollector(FakeLogStream(), FakeLogSink(), block = Duration.ZERO, clock = { now })
        var authRetention: RetentionResult? = null
        val health = CollectorHealth(collector, { 0L }, { 0L }, { null }, { null }, lastAuthRetention = { authRetention }, startedAt = now)
        collector.runOnce()
        health.report(now).body.let {
            assertEquals("", it["last_auth_retention_at"])
            assertEquals(-1, it["last_auth_retention_deleted"])
            assertEquals("", it["last_auth_retention_failure"])
        }
        authRetention = RetentionResult(now, null, "QueryTimeoutException")
        val report = health.report(now)
        assertTrue(report.healthy, "시도 기록 삭제 실패가 수집 판정을 바꿨다")
        assertEquals(now.toString(), report.body["last_auth_retention_at"])
        assertEquals(-1, report.body["last_auth_retention_deleted"])
        assertEquals("QueryTimeoutException", report.body["last_auth_retention_failure"])
    }
}
