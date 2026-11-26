package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.logalert.LogAlertRound
import xyz.junproject.observe.logalert.LogAlertSettings
import xyz.junproject.observe.logalert.LogAlertStore
import xyz.junproject.observe.logalert.LogJudgeResult
import xyz.junproject.observe.logalert.LogRuleState
import xyz.junproject.observe.logalert.LogSilentJudge
import xyz.junproject.observe.logalert.WatchedService
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.LogAlerts
import xyz.junproject.observe.threshold.LogRule
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RuleState
import xyz.junproject.observe.threshold.ThresholdStore
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 실제 PostgreSQL — 로그 무수신 판정의 저장소(`log_rule_state` — 자원 판정의 상태 표와 따로):
 * 표의 꼴(열쇠 = 서비스 · 규칙) · 한 바퀴가 잠금(자원 판정과 다른 잠금) · 상태 · 보낼 알림(자원 알림과 같은 보낼 표)을 한 트랜잭션에 · 도중의 실패는 통째로 되돌림 ·
 * 서비스마다의 마지막 로그(로그 표의 `stream_ms` — Redis 가 받은 시각, 서비스 색인으로)와 전체의 마지막 로그 · 자원 끊김이 넘어 있는 서버(`resource_rule_state`) ·
 * 두 자리가 판정 시각을 달리해 함께 돌아도 넘음 알림은 하나 · 판정의 조회는 시간 상한에서 끊기고 그 상한이 풀의 연결에 남지 않는다. 표는 전용 스키마(`log_alert_store`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogAlertStoreEngineTest {
    private val schema = "log_alert_store"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()

    private fun pool(size: Int) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@LogAlertStoreEngineTest.schema},public"   // 로그 표의 글 색인(pg_trgm)이 public 에 있다
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
    }).also { testPools += it }

    private val mainPool = pool(5)
    private val jdbc = JdbcTemplate(mainPool)
    private val store = LogAlertStore(jdbc)
    private val thresholdStore = ThresholdStore(jdbc)
    private val logStore = LogStore(jdbc, jdbc, jdbc)
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val settings = LogAlertSettings(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30))), null, emptyList())

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        thresholdStore.createSchema()
        store.createSchema()
    }

    @AfterEach fun closeExtraPools() { testPools.filter { it !== mainPool }.forEach { it.close() }; testPools.retainAll(listOf(mainPool)) }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        testPools.forEach { it.close() }
        administration.close()
    }

    private fun prepareLogTable() = JdbcTemplate(pool(1)).let { LogStore(it, it, it) }.createSchema()

    private fun logLine(service: String, at: Instant, sequence: Int = 0) =
        LogEntry(StreamId(at.toEpochMilli(), sequence), "info", service, "", "본문")

    private fun outboxKeys() = jdbc.queryForList("SELECT alert_key FROM resource_alert_outbox ORDER BY id", String::class.java)

    // ── 표와 한 바퀴 ──────────────────────────────────

    @Test
    fun `상태 표는 서비스 · 규칙이 열쇠이고 표 만들기를 두 번 해도 그대로다`() {
        store.createSchema()
        val columns = administrationJdbc.queryForList(
            """SELECT column_name || ' ' || data_type || ' ' || is_nullable FROM information_schema.columns
               WHERE table_schema = ? AND table_name = 'log_rule_state' ORDER BY ordinal_position""", String::class.java, schema)
        assertEquals(listOf("service text NO", "rule text NO", "firing boolean NO", "since_ms bigint YES", "last_notified_ms bigint YES",
            "last_received_ms bigint YES"), columns)
        val key = administrationJdbc.queryForList(
            """SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
               WHERE i.indrelid = '$schema.log_rule_state'::regclass AND i.indisprimary ORDER BY a.attname""", String::class.java)
        assertEquals(listOf("rule", "service"), key)
        assertEquals(emptyList<LogRuleState>(), store.states())
    }

    @Test
    fun `한 바퀴가 상태와 알림을 함께 기록하고 다음 바퀴는 그 상태를 받으며 알림은 자원 알림과 같은 보낼 표에 든다`() {
        val alert = LogAlerts.silent("backend", "ci-agent-158", null, Duration.ofMinutes(30), now, resolved = false)
        val written = listOf(LogRuleState("backend", LogRule.SILENT, true, now, now, null),
            LogRuleState(LogSilentJudge.ALL_SERVICES, LogRule.SILENT_ALL, false, null, null, now.minusSeconds(5)))
        val outcome = store.judgeRound(now) { previous -> assertEquals(emptyList<LogRuleState>(), previous); LogJudgeResult(written, listOf(alert)) }
        assertEquals(true, outcome.judged)
        assertEquals(1, outcome.eventsCreated)
        assertEquals(written.toSet(), store.states().toSet())
        assertEquals(listOf(alert.key), outboxKeys())
        assertEquals(listOf(alert), thresholdStore.pendingByTarget(50).flatMap { queue -> queue.alerts.map { it.alert } })

        var seen: List<LogRuleState>? = null
        store.judgeRound(now.plusSeconds(30)) { previous -> seen = previous; LogJudgeResult(previous, emptyList()) }
        assertEquals(written.toSet(), seen?.toSet())
        assertTrue(thresholdStore.states().isEmpty(), "로그 판정이 자원 판정의 상태 표에 줄을 남겼다")
    }

    @Test
    fun `한 바퀴의 도중에 실패하면 상태도 알림도 남지 않고 예외로 낸다`() {
        val alert = LogAlerts.silent("backend", "ci-agent-158", null, Duration.ofMinutes(30), now, resolved = false)
        assertThrows(IllegalStateException::class.java) {
            store.judgeRound(now) { LogJudgeResult(listOf(LogRuleState("backend", LogRule.SILENT, true, now, now, null)), listOf(alert)).also { throw IllegalStateException("판정 도중") } }
        }
        assertEquals(emptyList<LogRuleState>(), store.states())
        assertEquals(emptyList<String>(), outboxKeys())
    }

    // ── 판정이 읽는 것 ─────────────────────────────────

    @Test
    fun `서비스마다 마지막으로 받은 로그의 시각을 이름이 정확히 같은 줄에서 읽고 없으면 null 이며 전체의 마지막도 읽는다`() {
        prepareLogTable()
        assertNull(store.lastReceivedAny(), "로그가 하나도 없는데 시각이 나왔다")
        logStore.insertAll(listOf(logLine("backend", now.minusSeconds(600)), logLine("backend", now.minusSeconds(60), 1),
            logLine("Backend", now.minusSeconds(1)), logLine("front", now.minusSeconds(300))))
        assertEquals(mapOf("backend" to now.minusSeconds(60), "front" to now.minusSeconds(300), "ghost" to null),
            store.lastReceived(listOf("backend", "front", "ghost")))
        assertEquals(now.minusSeconds(1), store.lastReceivedAny())
    }

    @Test
    fun `자원 끊김이 넘어 있는 서버만 끊긴 서버로 읽고 자원 판정의 표가 없으면 비어 있다`() {
        thresholdStore.judgeRound(now) {
            JudgeResult(listOf(
                RuleState("ci-agent-158", ResourceRule.SILENT, "", null, true, now, now, null),
                RuleState("ci-agent-9", ResourceRule.SILENT, "", null, false, null, now, null),
                RuleState("ci-agent-164", ResourceRule.MEMORY, "", null, true, now, now, 95.0),
            ), emptyList())
        }
        assertEquals(setOf("ci-agent-158"), store.silentServers())
        administrationJdbc.execute("DROP TABLE $schema.resource_rule_state")
        assertEquals(emptySet<String>(), store.silentServers())
    }

    // ── 두 자리 · 잠금 ─────────────────────────────────

    @Test
    fun `두 자리가 판정 시각을 달리해 함께 여러 번 돌아도 한 서비스의 넘음 알림은 하나이고 잠금을 얻지 못한 자리는 실패가 아니라 판정하지 않음으로 끝난다`() {
        prepareLogTable()
        val slots = listOf(LogAlertStore(JdbcTemplate(pool(3))), LogAlertStore(JdbcTemplate(pool(3))))
        val rounds = slots.map { slot -> LogAlertRound(settings, slot, slot, LogSilentJudge(settings)) { true } }
        val executor = Executors.newFixedThreadPool(2)
        try {
            val start = CountDownLatch(1)
            val runs = rounds.mapIndexed { index, round ->
                executor.submit<List<String?>> { start.await(); (0 until 20).map { step -> round.judgeOnce(now.plusMillis(index * 7L + step * 1_000L)).failure } }
            }
            start.countDown()
            val failures = runs.flatMap { it.get(60, TimeUnit.SECONDS) }.filterNotNull()
            assertEquals(emptyList<String>(), failures, "두 자리가 함께 돌다 판정이 실패했다(한 자리씩 판정하지 않았다)")
        } finally {
            executor.shutdownNow()
        }
        assertEquals(listOf("log:backend:silent"), outboxKeys(), "두 자리가 넘음 알림을 두 번 만들었다")
        assertEquals(1, store.states().count { it.service == "backend" && it.firing })
    }

    @Test
    fun `자원 판정이 판정 잠금을 쥐고 있어도 로그 판정은 기다리지 않고 돈다`() {
        val inside = CountDownLatch(1)
        val release = CountDownLatch(1)
        val resource = CompletableFuture.runAsync {
            ThresholdStore(JdbcTemplate(pool(2))).judgeRound(now) { inside.countDown(); release.await(10, TimeUnit.SECONDS); JudgeResult(emptyList(), emptyList()) }
        }
        try {
            assertTrue(inside.await(10, TimeUnit.SECONDS))
            val outcome = CompletableFuture.supplyAsync { store.judgeRound(now) { LogJudgeResult(it, emptyList()) } }.get(5, TimeUnit.SECONDS)
            assertTrue(outcome.judged, "로그 판정이 자원 판정의 잠금에 막혔다")
        } finally {
            release.countDown()
            resource.get(10, TimeUnit.SECONDS)
        }
    }

    // ── 시간 상한 ──────────────────────────────────────

    private fun statementTimeouts(target: HikariDataSource, size: Int): List<String> {
        val borrowed = (1..size).map { target.connection }
        try {
            return borrowed.map { connection -> connection.createStatement().use { statement -> statement.executeQuery("SHOW statement_timeout").use { it.next(); it.getString(1) } } }
        } finally {
            borrowed.forEach { it.close() }
        }
    }

    @Test
    fun `판정의 조회는 시간 상한에서 끊겨 그 바퀴가 실패로 끝나고 상한은 풀의 어느 연결에도 남지 않는다`() {
        prepareLogTable()
        val limitedPool = pool(3)
        val limited = LogAlertStore(JdbcTemplate(limitedPool), readTimeout = Duration.ofSeconds(1))
        val before = statementTimeouts(limitedPool, 3)
        val holder: Connection = administration.connection
        holder.autoCommit = false
        holder.createStatement().use { it.execute("LOCK TABLE $schema.log_entry IN ACCESS EXCLUSIVE MODE") }
        val started = System.nanoTime()
        try {
            val failure = CompletableFuture.supplyAsync {
                runCatching { limited.judgeRound(now) { previous -> limited.lastReceived(listOf("backend")); LogJudgeResult(previous, emptyList()) } }
            }.get(15, TimeUnit.SECONDS).exceptionOrNull()
            val elapsed = Duration.ofNanos(System.nanoTime() - started)
            assertNotNull(failure, "로그 표가 잠겼는데 판정의 조회가 끊기지 않았다")
            assertTrue(elapsed < Duration.ofMillis(1_900), "판정의 조회가 1초 상한에서 끊기지 않았다(${elapsed.toMillis()}ms)")
        } finally {
            holder.rollback(); holder.autoCommit = true; holder.close()
        }
        assertEquals(before, statementTimeouts(limitedPool, 3), "판정의 시간 상한이 풀의 연결에 남았다")
        limited.judgeRound(now) { previous -> assertEquals(mapOf("backend" to null), limited.lastReceived(listOf("backend"))); LogJudgeResult(previous, emptyList()) }
    }
}
