package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.auth.AuthAttemptController
import xyz.junproject.observe.auth.AuthAttemptCursor
import xyz.junproject.observe.auth.AuthAttemptPolicy
import xyz.junproject.observe.auth.AuthAttemptQuery
import xyz.junproject.observe.auth.AuthAttemptRequest
import xyz.junproject.observe.auth.AuthAttemptRow
import xyz.junproject.observe.auth.AuthAttemptStore
import xyz.junproject.observe.auth.AuthOutcome
import xyz.junproject.observe.auth.AuthRejectedAttempt
import xyz.junproject.observe.auth.AuthVerdict
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogQuery
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 실제 PostgreSQL — 명세 ② 잠금·시도 기록·관측 서버 추가분, ⑤ 자동(observe):
 * 연속 5회 실패 → 잠금, 성공 시 초기화, 잠금 만료, 동시에 들어온 시도의 횟수, 같은 attempt_id 재전송, 90일 삭제,
 * 새 스키마가 기존 log_entry 를 건드리지 않음. 시각은 요청의 `at`·조회의 `now` 로 정한다(sleep 없음).
 *
 * 각 사례는 빈 전용 스키마(`auth_engine`)에서 돈다 — 구현의 표 이름을 몰라도 사례끼리 섞이지 않는다.
 * 기존 표와의 공존만은 실제 운영과 같은 기본 스키마에서 본다(마지막 사례).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuthAttemptEngineTest {
    private val schema = "auth_engine"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=$schema"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 8
    })
    private val policy = AuthAttemptPolicy()
    private val store = AuthAttemptStore(JdbcTemplate(dataSource), policy)
    private val start = Instant.parse("2026-11-22T03:00:00Z")
    private val lockDuration = Duration.ofMinutes(15)

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private fun attempt(at: Instant, credentialOk: Boolean, replayed: Boolean = false, id: UUID = UUID.randomUUID()) =
        AuthAttemptRequest(id, at, credentialOk, "203.0.113.7", "Mozilla/5.0", replayed)

    private fun fail(at: Instant) = store.decide(attempt(at, credentialOk = false))

    private fun rejected(at: Instant, id: UUID = UUID.randomUUID()) = AuthRejectedAttempt(id, at, "rate_limited", "198.51.100.9", "curl/8")

    /** 저장소 목록을 끝까지 따라 읽는다 */
    private fun allRows(from: Instant = Instant.parse("2000-01-01T00:00:00Z"), to: Instant = Instant.parse("2100-01-01T00:00:00Z")): List<AuthAttemptRow> {
        val collected = mutableListOf<AuthAttemptRow>()
        var query = AuthAttemptQuery(from, to, null, 200)
        while (true) {
            val page = store.list(query)
            collected += page
            if (page.size < query.limit) return collected
            query = query.copy(before = AuthAttemptCursor(page.last().at, page.last().id))
            check(collected.size < 100_000) { "목록이 끝나지 않는다" }
        }
    }

    // ── 잠금 ──────────────────────────────────────────────

    @Test
    fun `연속 5회 실패하면 다섯 번째에서 잠금이 시작되고 끝은 그 시각 + 15분이다`() {
        val verdicts = (0 until 5).map { fail(start.plusSeconds(it * 10L)) }
        assertEquals(listOf(1, 2, 3, 4, 5), verdicts.map { it.failures })
        assertEquals(listOf(false, false, false, false, true), verdicts.map { it.lockStarted })
        assertTrue(verdicts.none { it.allowed })
        val fifthAt = start.plusSeconds(40)
        assertEquals(fifthAt.plus(lockDuration), verdicts.last().lockedUntil)
        assertEquals(listOf(null, null, null, null), verdicts.take(4).map { it.lockedUntil })

        val status = store.lockStatus(fifthAt.plusSeconds(1))
        assertTrue(status.locked)
        assertEquals(fifthAt.plus(lockDuration), status.lockedUntil)
        assertEquals(5, status.failures)
    }

    @Test
    fun `잠금 중에는 맞는 비밀번호도 locked 로 거절되고 기록은 남지만 횟수·잠금 끝은 그대로다`() {
        (0 until 5).forEach { fail(start.plusSeconds(it.toLong())) }
        val until = start.plusSeconds(4).plus(lockDuration)

        val rightPassword = store.decide(attempt(start.plus(Duration.ofMinutes(1)), credentialOk = true))
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.LOCKED, lockedUntil = until, lockStarted = false, failures = 5), rightPassword)

        val wrongJustBeforeEnd = fail(until.minusMillis(1))
        assertEquals(AuthOutcome.LOCKED, wrongJustBeforeEnd.outcome)
        assertFalse(wrongJustBeforeEnd.lockStarted, "이미 걸린 잠금에서 lock_started 가 다시 켜졌다 — 알림이 겹친다")
        assertEquals(until, wrongJustBeforeEnd.lockedUntil, "잠금 중의 실패가 잠금을 연장했다")
        assertEquals(5, wrongJustBeforeEnd.failures, "잠금 중의 실패가 횟수를 늘렸다")

        val status = store.lockStatus(until.minusMillis(1))
        assertEquals(true to until, status.locked to status.lockedUntil)
        assertEquals(5, status.failures)

        val rows = allRows()
        assertEquals(7, rows.size, "잠금 중의 시도도 기록한다")
        assertEquals(2, rows.count { it.outcome == AuthOutcome.LOCKED })
    }

    @Test
    fun `성공하면 실패 횟수가 0 이 되어 다시 5회를 채워야 잠긴다`() {
        (0 until 4).forEach { fail(start.plusSeconds(it.toLong())) }
        val success = store.decide(attempt(start.plusSeconds(10), credentialOk = true))
        assertEquals(AuthVerdict(allowed = true, outcome = AuthOutcome.SUCCESS, lockedUntil = null, lockStarted = false, failures = 0), success)
        assertEquals(0, store.lockStatus(start.plusSeconds(11)).failures)

        val afterReset = (0 until 4).map { fail(start.plusSeconds(20L + it)) }
        assertEquals(listOf(1, 2, 3, 4), afterReset.map { it.failures })
        assertTrue(afterReset.none { it.lockStarted })
        assertFalse(store.lockStatus(start.plusSeconds(30)).locked)
    }

    @Test
    fun `잠금이 끝난 뒤에는 0 부터 센다`() {
        (0 until 5).forEach { fail(start.plusSeconds(it.toLong())) }
        val until = start.plusSeconds(4).plus(lockDuration)
        assertFalse(store.lockStatus(until).locked, "잠금 끝 시각에는 풀려 있어야 한다")

        val first = fail(until)
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.FAILURE, lockedUntil = null, lockStarted = false, failures = 1), first)
        val next = (1 until 5).map { fail(until.plusSeconds(it.toLong())) }
        assertEquals(listOf(2, 3, 4, 5), next.map { it.failures })
        assertTrue(next.last().lockStarted, "풀린 뒤 다시 5회를 채웠는데 잠기지 않았다")
        assertEquals(until.plusSeconds(4).plus(lockDuration), next.last().lockedUntil)
    }

    @Test
    fun `잠금이 끝난 뒤의 맞는 비밀번호는 허용된다`() {
        (0 until 5).forEach { fail(start.plusSeconds(it.toLong())) }
        val until = start.plusSeconds(4).plus(lockDuration)
        val verdict = store.decide(attempt(until.plusSeconds(1), credentialOk = true))
        assertTrue(verdict.allowed)
        assertEquals(AuthOutcome.SUCCESS, verdict.outcome)
        assertEquals(0, verdict.failures)
    }

    // ── 동시성 ────────────────────────────────────────────

    /** [count] 건을 출발선에 세워 한꺼번에 놓는다 */
    private fun <T> concurrently(count: Int, work: (Int) -> T): List<T> {
        val executor = Executors.newFixedThreadPool(count)
        try {
            val ready = CountDownLatch(count)
            val go = CountDownLatch(1)
            val futures = (0 until count).map { index ->
                executor.submit<T> { ready.countDown(); go.await(); work(index) }
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS))
            go.countDown()
            return futures.map { it.get(30, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `동시에 들어온 실패 5건 — 정확히 한 건만 lock_started 이고 최종 횟수는 5`() {
        // 엇갈림은 운에 달려 있다 — 여러 번 되풀이해 한 번이라도 깨지면 실패
        repeat(20) { round ->
            reset()
            val verdicts = concurrently(5) { index -> fail(start.plusMillis(index.toLong())) }
            assertEquals(1, verdicts.count { it.lockStarted }, "round $round: lock_started 수가 1 이 아니다 — $verdicts")
            assertEquals(listOf(1, 2, 3, 4, 5), verdicts.map { it.failures }.sorted(), "round $round: 횟수가 겹치거나 빠졌다 — $verdicts")
            val locked = verdicts.single { it.lockStarted }
            assertTrue(locked.lockedUntil in (0 until 5).map { start.plusMillis(it.toLong()).plus(lockDuration) }, "round $round: ${locked.lockedUntil}")
            val status = store.lockStatus(start.plusSeconds(1))
            assertEquals(true to 5, status.locked to status.failures, "round $round")
            assertEquals(locked.lockedUntil, status.lockedUntil)
            assertEquals(5, allRows().size, "round $round: 기록이 빠졌다")
        }
    }

    @Test
    fun `동시에 들어온 실패 8건 — 잠금은 한 번, 횟수는 5 에서 멈추고 나머지는 locked`() {
        repeat(10) { round ->
            reset()
            val verdicts = concurrently(8) { index -> fail(start.plusMillis(index.toLong())) }
            assertEquals(1, verdicts.count { it.lockStarted }, "round $round: $verdicts")
            assertEquals(3, verdicts.count { it.outcome == AuthOutcome.LOCKED }, "round $round: $verdicts")
            assertEquals(5, store.lockStatus(start.plusSeconds(1)).failures, "round $round")
            assertEquals(8, allRows().size)
        }
    }

    @Test
    fun `같은 attempt_id 가 동시에 다섯 번 와도 기록 1건·횟수 1 이고 판정이 모두 같다`() {
        repeat(10) { round ->
            reset()
            val request = attempt(start, credentialOk = false)
            val verdicts = concurrently(5) { store.decide(request) }
            assertEquals(1, verdicts.toSet().size, "round $round: 재전송의 판정이 다르다 — $verdicts")
            assertEquals(1, verdicts.first().failures)
            assertEquals(1, allRows().size, "round $round")
            assertEquals(1, store.lockStatus(start.plusSeconds(1)).failures)
        }
    }

    // ── 재전송 · replayed · 거절 기록 ─────────────────────

    @Test
    fun `같은 attempt_id 를 다시 보내면 처음 판정 그대로이고 새로 세지 않는다`() {
        val id = UUID.randomUUID()
        val first = store.decide(attempt(start, credentialOk = false, id = id))
        (1 until 4).forEach { fail(start.plusSeconds(it.toLong())) }   // 그 사이 상태가 바뀐다 — 횟수 4

        val again = store.decide(attempt(start, credentialOk = false, id = id))
        assertEquals(first, again)
        // 본문이 달라도(응답을 잃고 고쳐 보낸 꼴) 같은 ID 면 처음 것
        assertEquals(first, store.decide(attempt(start.plusSeconds(30), credentialOk = true, id = id)))
        assertEquals(4, store.lockStatus(start.plusSeconds(31)).failures, "재전송이 횟수를 바꿨다")
        assertEquals(4, allRows().size, "재전송이 기록을 늘렸다")
        assertEquals(1, allRows().count { it.id == id })
    }

    @Test
    fun `잠금을 시작한 시도를 다시 보내도 lock_started 그대로 — 알림 key 가 같은 locked_until 을 받는다`() {
        val ids = (0 until 5).map { UUID.randomUUID() }
        val verdicts = ids.mapIndexed { index, id -> store.decide(attempt(start.plusSeconds(index.toLong()), credentialOk = false, id = id)) }
        val resent = store.decide(attempt(start.plusSeconds(4), credentialOk = false, id = ids.last()))
        assertEquals(verdicts.last(), resent)
        assertTrue(resent.lockStarted)
        assertEquals(5, allRows().size)
    }

    @Test
    fun `replayed 실패는 기록만 하고 횟수에 넣지 않는다`() {
        (0 until 4).forEach { fail(start.plusSeconds(it.toLong())) }
        val replayed = store.decide(attempt(start.plusSeconds(10), credentialOk = false, replayed = true))
        assertFalse(replayed.lockStarted, "replayed 가 잠금을 걸었다")
        assertEquals(4, store.lockStatus(start.plusSeconds(11)).failures, "replayed 가 횟수를 늘렸다")
        assertFalse(store.lockStatus(start.plusSeconds(11)).locked)

        val fifth = fail(start.plusSeconds(20))
        assertTrue(fifth.lockStarted, "replayed 를 빼면 이것이 다섯 번째 실패다")
        assertEquals(5, fifth.failures)

        val row = allRows().single { it.replayed }
        assertEquals(start.plusSeconds(10), row.at, "원래 시각으로 남는다")
        assertEquals(AuthOutcome.FAILURE, row.outcome)
    }

    @Test
    fun `replayed 성공은 실패 횟수를 0 으로 되돌리지 않는다`() {
        (0 until 4).forEach { fail(start.plusSeconds(it.toLong())) }
        store.decide(attempt(start.plusSeconds(10), credentialOk = true, replayed = true))
        assertEquals(4, store.lockStatus(start.plusSeconds(11)).failures, "replayed 성공이 잠금 계산에 들어갔다")
        assertTrue(fail(start.plusSeconds(20)).lockStarted)
        assertEquals(AuthOutcome.SUCCESS, allRows().single { it.replayed }.outcome)
    }

    @Test
    fun `replayed 는 잠금 중에도 잠금 끝을 바꾸지 않는다`() {
        (0 until 5).forEach { fail(start.plusSeconds(it.toLong())) }
        val until = start.plusSeconds(4).plus(lockDuration)
        store.decide(attempt(start.plusSeconds(60), credentialOk = false, replayed = true))
        store.decide(attempt(start.plusSeconds(61), credentialOk = true, replayed = true))
        val status = store.lockStatus(start.plusSeconds(62))
        assertEquals(Triple(true, until, 5), Triple(status.locked, status.lockedUntil, status.failures))
    }

    @Test
    fun `속도 제한 거절은 기록만 하고 잠금 계산에 들지 않는다`() {
        (0 until 4).forEach { fail(start.plusSeconds(it.toLong())) }
        (0 until 3).forEach { store.recordRejected(rejected(start.plusSeconds(10L + it))) }
        assertEquals(4, store.lockStatus(start.plusSeconds(20)).failures)
        assertFalse(store.lockStatus(start.plusSeconds(20)).locked)

        val rows = allRows()
        assertEquals(7, rows.size)
        val rejectedRows = rows.filter { it.outcome == AuthOutcome.RATE_LIMITED }
        assertEquals(3, rejectedRows.size)
        assertTrue(rejectedRows.all { it.ip == "198.51.100.9" && it.userAgent == "curl/8" && !it.replayed })
        assertTrue(fail(start.plusSeconds(30)).lockStarted)
    }

    @Test
    fun `같은 attempt_id 의 거절 기록을 다시 보내도 1건`() {
        val id = UUID.randomUUID()
        store.recordRejected(rejected(start, id))
        store.recordRejected(rejected(start, id))
        assertEquals(1, allRows().size)
    }

    @Test
    fun `기록에는 시각·결과·IP·User-Agent 가 그대로 남는다`() {
        val id = UUID.randomUUID()
        val userAgent = "가".repeat(200)
        store.decide(AuthAttemptRequest(id, Instant.parse("2026-11-22T03:00:00.123Z"), false, "2001:db8::1", userAgent, false))
        assertEquals(AuthAttemptRow(id, Instant.parse("2026-11-22T03:00:00.123Z"), AuthOutcome.FAILURE, "2001:db8::1", userAgent, false), allRows().single())
    }

    // ── 목록 ──────────────────────────────────────────────

    private val controller = AuthAttemptController(store, "secret")

    /** 경로를 따라 끝까지 읽는다 — 응답의 next_before 를 다음 before 로, from·to 는 그대로 */
    @Suppress("UNCHECKED_CAST")
    private fun readAllPages(from: Instant, to: Instant, limit: Int, now: Instant): List<Map<String, Any?>> {
        val collected = mutableListOf<Map<String, Any?>>()
        var before: String? = null
        var pages = 0
        while (true) {
            val response = controller.list(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
                from.toString(), to.toString(), before, limit.toString(), now)
            assertEquals(200, response.statusCode.value(), "${response.body}")
            val data = (response.body as Map<String, Any?>)["data"] as Map<String, Any?>
            val page = data["attempts"] as List<Map<String, Any?>>
            assertTrue(page.size <= limit)
            collected += page
            before = (data["next_before"] as String).ifEmpty { return collected }
            check(++pages < 1_000) { "이어 보기가 끝나지 않는다" }
        }
    }

    @Test
    fun `이어 보기로 끝까지 읽으면 최신순이고 같은 시각이 여럿이어도 중복·누락이 없다`() {
        // 같은 시각에 여러 건 — 쪽 경계가 그 사이에 걸리게 한다. 판정 기록과 거절 기록을 섞는다
        val ids = mutableListOf<String>()
        (0 until 120).forEach { index ->
            val at = start.plusSeconds((index / 4).toLong())
            val id = UUID.randomUUID()
            if (index % 3 == 0) store.decide(attempt(at, credentialOk = index % 2 == 0, id = id)) else store.recordRejected(rejected(at, id))
            ids += id.toString()
        }
        val now = start.plus(Duration.ofHours(1))
        for (limit in listOf(7, 4, 50, 200)) {
            val rows = readAllPages(start.minusSeconds(1), now, limit, now)
            assertEquals(ids.size, rows.size, "limit $limit: 건수")
            assertEquals(ids.toSet(), rows.map { it["id"] }.toSet(), "limit $limit: 중복 또는 누락")
            val times = rows.map { Instant.parse(it["at"] as String) }
            assertEquals(times.sortedDescending(), times, "limit $limit: 최신순이 아니다")
        }
    }

    @Test
    fun `목록은 기간 밖을 넣지 않고 경계 시각은 넣는다`() {
        val inside = listOf(start, start.plusSeconds(30), start.plusSeconds(60))
        inside.forEach { store.recordRejected(rejected(it)) }
        store.recordRejected(rejected(start.minusMillis(1)))
        store.recordRejected(rejected(start.plusSeconds(60).plusMillis(1)))
        val rows = store.list(AuthAttemptQuery(start, start.plusSeconds(60), null, 200))
        assertEquals(inside.reversed(), rows.map { it.at })
    }

    @Test
    fun `limit 상한 200 — 그보다 많아도 한 쪽에 200 건`() {
        (0 until 230).forEach { store.recordRejected(rejected(start.plusMillis(it.toLong()))) }
        val now = start.plus(Duration.ofHours(1))
        @Suppress("UNCHECKED_CAST")
        val data = (controller.list(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
            start.minusSeconds(1).toString(), now.toString(), null, "200", now).body as Map<String, Any?>)["data"] as Map<String, Any?>
        assertEquals(200, (data["attempts"] as List<*>).size)
        assertNotEquals("", data["next_before"])
        assertEquals(422, controller.list(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
            start.minusSeconds(1).toString(), now.toString(), null, "201", now).statusCode.value())
    }

    @Test
    fun `기간이 90일을 넘으면 422`() {
        val now = start
        val response = controller.list(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
            now.minus(Duration.ofDays(90)).minusMillis(1).toString(), now.toString(), null, null, now)
        assertEquals(422, response.statusCode.value())
    }

    @Test
    fun `판정 경로를 실제 저장소로 거쳐도 봉투와 다섯 칸이 나온다`() {
        val secret = MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }
        lateinit var last: Map<String, Any?>
        (0 until 5).forEach { index ->
            val response = controller.decide(secret, mapOf(
                "attempt_id" to UUID.randomUUID().toString(), "at" to start.plusSeconds(index.toLong()).toString(),
                "credential_ok" to false, "ip" to "203.0.113.7", "user_agent" to "Mozilla/5.0",
            ))
            assertEquals(200, response.statusCode.value(), "${response.body}")
            @Suppress("UNCHECKED_CAST")
            last = (response.body as Map<String, Any?>)["data"] as Map<String, Any?>
        }
        assertEquals(mapOf("allowed" to false, "outcome" to "failure", "locked_until" to start.plusSeconds(4).plus(lockDuration).toString(),
            "lock_started" to true, "failures" to 5), last)
        @Suppress("UNCHECKED_CAST")
        val lock = (controller.lock(secret, start.plusSeconds(5)).body as Map<String, Any?>)["data"] as Map<String, Any?>
        assertEquals(mapOf("locked" to true, "locked_until" to start.plusSeconds(4).plus(lockDuration).toString(), "failures" to 5), lock)
    }

    // ── 90일 삭제 ─────────────────────────────────────────

    @Test
    fun `90일 삭제는 정확히 90일 된 기록을 남기고 그보다 1ms 오래된 기록을 지운다`() {
        val now = Instant.parse("2026-11-22T03:00:00Z")
        val boundary = now.minus(policy.retention)
        store.recordRejected(rejected(boundary.minusMillis(1)))
        store.decide(attempt(boundary.minusMillis(1), credentialOk = false))
        store.recordRejected(rejected(boundary))
        store.recordRejected(rejected(boundary.plusMillis(1)))
        store.recordRejected(rejected(now))

        assertEquals(2, store.deleteOlderThan(now.minus(policy.retention)))
        assertEquals(listOf(now, boundary.plusMillis(1), boundary), allRows().map { it.at })
        assertEquals(0, store.deleteOlderThan(now.minus(policy.retention)), "두 자리가 함께 돌아도 결과가 같다")
        assertEquals(3, allRows().size)
    }

    // ── 기존 log_entry 와의 공존(기본 스키마) ──────────────

    @Test
    fun `새 스키마 생성이 기존 log_entry 의 표·인덱스·행을 건드리지 않고 수집·조회가 그대로다`() {
        val logStore = EngineSupport.store(administration)
        EngineSupport.resetTable(administration)
        val logTime = Instant.parse("2026-11-22T02:30:00Z").toEpochMilli()
        logStore.insertAll((0 until 10).map { LogEntry(StreamId(logTime + it, 0), "error", "backend", "req-$it", "message $it") })

        fun logColumns() = administrationJdbc.queryForList(
            "SELECT column_name || ' ' || data_type || ' ' || is_nullable FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'log_entry' ORDER BY ordinal_position",
            String::class.java)
        fun logIndexes() = administrationJdbc.queryForList(
            "SELECT indexname || ' ' || indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'log_entry' ORDER BY indexname", String::class.java)
        fun tables() = administrationJdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY table_name", String::class.java).toSet()

        val columnsBefore = logColumns()
        val indexesBefore = logIndexes()
        val tablesBefore = tables()
        val publicStore = AuthAttemptStore(administrationJdbc, policy)
        try {
            publicStore.createSchema()
            publicStore.createSchema()   // 두 자리가 차례로 떠도 그대로
            assertEquals(columnsBefore, logColumns(), "log_entry 의 열이 바뀌었다")
            assertEquals(indexesBefore, logIndexes(), "log_entry 의 인덱스가 바뀌었다")
            assertTrue(tables().containsAll(tablesBefore), "기존 표가 사라졌다")

            // 기존 수집·조회가 그대로 — 새 스키마 뒤에 LogStore 가 다시 떠도(구버전 자리) 동작한다
            logStore.createSchema()
            assertEquals(1, logStore.insertAll(listOf(LogEntry(StreamId(logTime + 100, 0), "info", "front", "req-new", "after auth schema"))))
            val found = logStore.search(LogQuery(Instant.ofEpochMilli(logTime - 1), Instant.ofEpochMilli(logTime + 1_000), limit = 500))
            assertEquals(11, found.size)
            assertEquals(10L, logStore.stats(Instant.ofEpochMilli(logTime - 1), Instant.ofEpochMilli(logTime + 1_000)).filter { it.level == "error" }.sumOf { it.count })

            // 두 삭제가 서로의 표를 지우지 않는다
            publicStore.recordRejected(rejected(Instant.parse("2026-01-01T00:00:00Z")))
            publicStore.deleteOlderThan(Instant.parse("2100-01-01T00:00:00Z"))
            assertEquals(11, logStore.search(LogQuery(Instant.ofEpochMilli(logTime - 1), Instant.ofEpochMilli(logTime + 1_000), limit = 500)).size,
                "시도 기록 삭제가 로그를 지웠다")
            val kept = UUID.randomUUID()
            publicStore.recordRejected(rejected(Instant.parse("2026-01-01T00:00:00Z"), kept))
            logStore.deleteOlderThan(Instant.parse("2100-01-01T00:00:00Z"))
            assertEquals(listOf(kept), publicStore.list(AuthAttemptQuery(Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z"), null, 200)).map { it.id },
                "로그 삭제가 시도 기록을 지웠다")
        } finally {
            // 이 사례가 기본 스키마에 더한 표만 지운다 — 다른 엔진 테스트가 보는 log_entry 는 남긴다
            (tables() - tablesBefore).forEach { administrationJdbc.execute("DROP TABLE IF EXISTS \"$it\" CASCADE") }
        }
    }
}
