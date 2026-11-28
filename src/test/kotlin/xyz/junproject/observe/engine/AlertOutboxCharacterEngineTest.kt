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
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * 실제 PostgreSQL — 서버에 이미 있는 보낼 표(`resource_alert_outbox`)를 지금 판이 어떻게 다루는지 고정한다(알림 타입을 넓히기 전의 특성):
 * 표의 꼴(칸 · 형 · NOT NULL · 기본값) · 지금까지의 판이 써 온 꼴(INSERT 의 칸 목록)로 넣은 줄이 그대로 읽히고 보내진다(모든 자원 규칙 · 등급) ·
 * 표가 이미 있을 때(서버의 지금) 표 만들기를 두 자리가 동시에 여러 번 해도 실패하지 않고 줄이 그대로다. 표는 전용 스키마(`outbox_character`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlertOutboxCharacterEngineTest {
    private val schema = "outbox_character"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()

    private fun pool(size: Int) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@AlertOutboxCharacterEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
    }).also { testPools += it }

    private val jdbc = JdbcTemplate(pool(6))
    private val store = ThresholdStore(jdbc)
    private val now = Instant.parse("2026-11-26T03:00:00Z")

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterEach fun closeExtraPools() { testPools.drop(1).forEach { it.close() }; testPools.retainAll(testPools.take(1)) }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        testPools.forEach { it.close() }
        administration.close()
    }

    private class RecordingSender : AlertSender {
        val sent = mutableListOf<ResourceAlert>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert; return SendOutcome.Delivered }
    }

    @Test
    fun `보낼 표의 칸 · 형 · NOT NULL · 기본값은 지금의 꼴이다`() {
        val columns = administrationJdbc.queryForList(
            """SELECT column_name || ' ' || data_type || ' ' || is_nullable || ' ' || (CASE WHEN column_default IS NULL THEN 'no-default' ELSE 'default' END)
                      || ' ' || is_identity
               FROM information_schema.columns WHERE table_schema = ? AND table_name = 'resource_alert_outbox' ORDER BY ordinal_position""",
            String::class.java, schema)
        assertEquals(listOf(
            "id bigint NO no-default YES",
            "event_id text NO no-default NO",
            "alert_key text NO no-default NO",
            "kind text NO no-default NO",
            "severity text NO no-default NO",
            "title text NO no-default NO",
            "detail text NO no-default NO",
            "server text NO no-default NO",
            "rule text NO no-default NO",
            "target_name text YES no-default NO",
            "created_ms bigint NO no-default NO",
            "attempts integer NO default NO",
            "last_failure text YES no-default NO",
            "last_attempt_ms bigint YES no-default NO",
            "rejected_ms bigint YES no-default NO",
        ), columns, "보낼 표의 꼴이 바뀌었다 — 옛 판의 INSERT(아래 칸 목록)가 깨지지 않는지 본다")
    }

    /** 지금까지의 판이 보낼 표에 쓰는 꼴 — 판정 한 바퀴의 INSERT 와 같은 칸 목록 */
    private fun insertAsWrittenSoFar(eventId: String, key: String, kind: String, severity: String, server: String, rule: String, targetName: String?, createdAt: Instant) =
        administrationJdbc.update(
            """INSERT INTO $schema.resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (event_id) DO NOTHING""",
            eventId, key, kind, severity, "$server 제목 $eventId", "자세히\n둘째 줄", server, rule, targetName, createdAt.toEpochMilli())

    @Test
    fun `지금까지의 판이 쓴 꼴의 줄은 자원 규칙 · 등급마다 그대로 읽히고 한 바퀴에 모두 보내진다`() {
        val expected = ResourceRule.entries.flatMapIndexed { index, rule ->
            AlertSeverity.entries.mapIndexed { level, severity ->
                val server = "server-$index-$level"
                val targetName = if (rule == ResourceRule.RESTARTS) "container-$level" else null
                val key = "resource:$server:${rule.wireName}" + (targetName?.let { ":$it" } ?: "")
                ResourceAlert("$key@1", key, rule.kind, severity, "$server 제목 $key@1", "자세히\n둘째 줄", server, rule, targetName,
                    now.plusMillis((index * 10 + level).toLong()))
            }
        }
        expected.forEach { insertAsWrittenSoFar(it.eventId, it.key, it.kind, it.severity.wireName, it.server, it.rule.wireName, it.targetName, it.createdAt) }

        assertEquals(expected.sortedBy { it.eventId }, store.pendingByTarget(50).flatMap { queue -> queue.alerts.map { it.alert } }.sortedBy { it.eventId },
            "지금까지의 꼴로 쓴 줄이 같은 값으로 읽히지 않는다")
        val sender = RecordingSender()
        val outcome = AlertDispatcher(store, sender).dispatch(now.plusSeconds(1))
        assertEquals(listOf(true, expected.size, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(expected.sortedBy { it.eventId }, sender.sent.sortedBy { it.eventId })
        assertEquals(0L, administrationJdbc.queryForObject("SELECT count(*) FROM $schema.resource_alert_outbox", Long::class.java))
    }

    @Test
    fun `표가 이미 있으면 두 자리가 동시에 여러 번 표 만들기를 해도 실패하지 않고 있던 줄이 그대로다`() {
        insertAsWrittenSoFar("kept-1", "resource:ci-agent-158:memory", "resource_memory", "warning", "ci-agent-158", "memory", null, now)
        val executor = Executors.newFixedThreadPool(2)
        try {
            repeat(5) { attempt ->
                val stores = listOf(ThresholdStore(JdbcTemplate(pool(2))), ThresholdStore(JdbcTemplate(pool(2))))
                val start = CountDownLatch(1)
                val runs = stores.map { slot -> executor.submit<Result<Unit>> { start.await(); runCatching { slot.createSchema(); slot.createSchema() } } }
                start.countDown()
                val failures = runs.mapNotNull { it.get(30, TimeUnit.SECONDS).exceptionOrNull() }
                assertEquals(emptyList<String>(), failures.map { "${it.javaClass.simpleName}: ${it.message?.take(200)}" }, "${attempt + 1}번째: 두 자리의 표 만들기가 실패했다")
                assertEquals(1L, administrationJdbc.queryForObject(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'resource_alert_outbox'", Long::class.java, schema))
                assertEquals(listOf("kept-1"), administrationJdbc.queryForList("SELECT event_id FROM $schema.resource_alert_outbox", String::class.java))
            }
        } finally {
            executor.shutdownNow()
        }
    }
}
