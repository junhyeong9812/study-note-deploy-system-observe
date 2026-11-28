package xyz.junproject.observe.engine

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.threshold.AlertCounts
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertRule
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.LogAlerts
import xyz.junproject.observe.threshold.LogRule
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant

/**
 * 실제 PostgreSQL — 보낼 표를 로그 알림과 함께 쓴다(02b): 로그 알림은 자원 알림과 같은 표 · 같은 꼴(칸을 더하지 않는다)로 들어 같은 값으로 읽히고 보내진다 ·
 * 보내기가 아는 규칙 글자 = [AlertRule.entries] 의 글자 전부(새로 아는 `log_silent` · `log_silent_all` 은 이제 보내고, 여전히 모르는 글자는 02a 처럼 건너뛰고 센다) ·
 * 등급 글자를 모르는 줄도 건너뛰고 센다(그 한 줄이 같은 바퀴의 다른 줄을 막지 않는다 — 지우지도 표시하지도 않는다) ·
 * 종류마다의 몫은 두지 않는다(대상의 차례가 그대로 — 로그 알림이 한 바퀴를 다 차지해도 자원 알림은 다음 바퀴에 나간다) · 다시 시도의 규칙이 로그 알림에도 같다 ·
 * 02a 가 먼저 배포됐다는 전제에서 새 판이 쓴 로그 알림 줄은 옛 판의 읽기에서 모르는 줄로 걸러진다. 표는 전용 스키마(`log_alert_outbox`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogAlertOutboxEngineTest {
    private val schema = "log_alert_outbox"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@LogAlertOutboxEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 6
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = ThresholdStore(jdbc)
    private val metricStore = MetricStore(jdbc, jdbc, jdbc)
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val threshold = Duration.ofMinutes(30)
    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        metricStore.createSchema()
        appender.list.clear()
    }

    @AfterAll
    fun close() {
        (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender)
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private class RecordingSender(private val outcome: (ResourceAlert) -> SendOutcome = { SendOutcome.Delivered }) : AlertSender {
        val sent = mutableListOf<ResourceAlert>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert; return outcome(alert) }
    }

    private fun resource(eventId: String, server: String, createdAt: Instant, rule: ResourceRule = ResourceRule.MEMORY) =
        ResourceAlert(eventId, "resource:$server:${rule.wireName}", rule.kind, AlertSeverity.WARNING, "$server 제목", "자세히", server, rule, null, createdAt)

    private fun enqueue(alerts: List<ResourceAlert>) = store.judgeRound(now) { JudgeResult(emptyList(), alerts) }

    private fun outboxRows(): List<Map<String, Any?>> = jdbc.queryForList("SELECT * FROM resource_alert_outbox ORDER BY id")

    private fun insertRow(eventId: String, rule: String, severity: String = "warning", server: String = "ci-agent-158", targetName: String? = "svc", createdAt: Instant = now) =
        jdbc.update(
            """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            eventId, "k:$eventId", "log_silent", severity, "제목 $eventId", "자세히", server, rule, targetName, createdAt.toEpochMilli())

    private fun health(round: ThresholdRound): Map<String, Any?> {
        val collector = MetricCollector(FakeMetricStream(), FakeMetricSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        return MetricHealth(collector, { 0L }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null }, startedAt = now, thresholds = round).report(now)
    }

    // ── 같은 표 · 같은 꼴 ─────────────────────────────

    @Test
    fun `로그 알림은 자원 알림과 같은 보낼 표에 들어 같은 값으로 읽히고 한 바퀴에 함께 보내진다`() {
        val alerts = listOf(
            LogAlerts.silent("backend", "ci-agent-158", now.minus(Duration.ofMinutes(40)), threshold, now, resolved = false),
            LogAlerts.silentAll(now.minus(Duration.ofMinutes(50)), threshold, now.plusMillis(1), resolved = false),
            resource("memory-1", "ci-agent-164", now.plusMillis(2)),
        )
        enqueue(alerts)
        val rows = outboxRows()
        assertEquals(listOf("log_silent", "log_silent_all", "memory"), rows.map { it["rule"] })
        assertEquals(listOf("log_silent", "log_silent", "resource_memory"), rows.map { it["kind"] })
        assertEquals(alerts.sortedBy { it.eventId }, store.pendingByTarget(50).flatMap { queue -> queue.alerts.map { it.alert } }.sortedBy { it.eventId },
            "보낼 표를 거친 로그 알림이 같은 값으로 읽히지 않는다")

        val sender = RecordingSender()
        val outcome = AlertDispatcher(store, sender).dispatch(now.plusSeconds(1))
        assertEquals(listOf(true, 3, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(alerts.map { it.eventBody() }.toSet(), sender.sent.map { it.eventBody() }.toSet(), "보낸 몸이 만든 알림과 다르다")
        assertEquals(emptyList<Map<String, Any?>>(), outboxRows())
        assertEquals(setOf("log_silent", "log_silent_all", "memory"),
            jdbc.queryForList("SELECT rule_group FROM resource_alert_target", String::class.java).toSet(), "로그 알림의 대상이 시각 표에 남지 않았다")
    }

    // ── 아는 글자의 경계가 정확히 옮겨진다 ────────────────

    @Test
    fun `보내기가 아는 규칙 글자는 AlertRule 의 글자 전부와 정확히 같고 그 밖의 글자는 건너뛰고 센다`() {
        val known = AlertRule.entries.map { it.wireName }
        known.forEachIndexed { index, rule -> insertRow("known-$index", rule, server = "server-$index") }
        val unknown = listOf("log_errors", "LOG_SILENT", "log_silent ", " log_silent", "log_silent_All", "")
        unknown.forEachIndexed { index, rule -> insertRow("unknown-$index", rule, server = "other-$index") }

        val read = store.pendingByTarget(50).flatMap { queue -> queue.alerts.map { it.alert.rule.wireName } }
        assertEquals(known.sorted(), read.sorted(), "보내기가 읽는 규칙 글자가 이 판이 아는 규칙과 다르다")
        assertEquals(AlertCounts((known.size + unknown.size).toLong(), 0, unknown.associateWith { 1L }), store.alertCounts(),
            "모르는 글자의 줄만 건너뛰고 세지 않았다")

        val sender = RecordingSender()
        assertEquals(known.size, AlertDispatcher(store, sender).dispatch(now.plusSeconds(1)).delivered)
        assertEquals(known.sorted(), sender.sent.map { it.rule.wireName }.sorted())
        assertEquals(unknown.sorted(), outboxRows().map { it["rule"] as String }.sorted(), "모르는 글자의 줄이 지워졌거나 아는 줄이 남았다")
        assertTrue(outboxRows().all { it["attempts"] == 0 && it["last_attempt_ms"] == null }, "모르는 글자의 줄이 표시됐다")
    }

    @Test
    fun `등급 글자를 모르는 줄은 건너뛰고 세며 같은 바퀴의 다른 줄은 보내지고 보내기 바퀴는 실패로 남지 않는다`() {
        insertRow("bad-resource", "memory", severity = "error", server = "ci-agent-158", targetName = null, createdAt = now)
        insertRow("bad-log", "log_silent", severity = "Warning", server = "ci-agent-158", targetName = "backend", createdAt = now.plusMillis(1))
        val good = listOf(resource("memory-1", "ci-agent-164", now.plusMillis(2)),
            LogAlerts.silent("front", "ci-agent-9", now.minus(Duration.ofHours(1)), threshold, now.plusMillis(3), resolved = false))
        enqueue(good)

        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), store, emptySet())
        val outcome = round.dispatchOnce(now.plusSeconds(1))
        assertNull(round.lastDispatchFailure, "등급 글자를 모르는 줄 때문에 보내기 바퀴가 실패했다")
        assertEquals(listOf(true, 2, 0, 0), outcome?.let { listOf(it.lockAcquired, it.delivered, it.retrying, it.rejected) })
        assertEquals(good.map { it.eventId }.toSet(), sender.sent.map { it.eventId }.toSet(), "같은 바퀴의 다른 줄이 보내지지 않았다")

        val left = outboxRows()
        assertEquals(setOf("bad-resource", "bad-log"), left.map { it["event_id"] }.toSet(), "등급 글자를 모르는 줄이 지워졌다")
        assertTrue(left.all { it["attempts"] == 0 && it["last_failure"] == null && it["rejected_ms"] == null }, "등급 글자를 모르는 줄이 표시됐다")
        assertEquals(2L, store.alertCounts().unknownRules.values.sum(), "등급 글자를 모르는 줄이 세어지지 않았다")
        assertEquals(2L, (health(round)["skipped_unknown_rule_alerts"] as? Number)?.toLong(), "헬스에 건너뛴 줄의 수가 드러나지 않았다")
        assertTrue(appender.list.any { it.level == Level.WARN && it.formattedMessage.contains("unknown rule") && it.formattedMessage.contains("total 2") },
            "건너뛴 줄이 WARN 에 드러나지 않았다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
    }

    @Test
    fun `같은 바퀴에 별표 이름의 서비스 · 전체 한 줄 · 숨은 문자만 다른 두 서비스 · 평범한 서비스가 넘으면 다섯 건이 모두 줄로 남고 한 바퀴에 모두 보내진다`() {
        val lastLog = now.minus(Duration.ofHours(1))
        val alerts = listOf(
            LogAlerts.silent("*", "ci-agent-158", lastLog, threshold, now, resolved = false),
            LogAlerts.silentAll(lastLog, threshold, now, resolved = false),
            LogAlerts.silent("a\u200Bb", "ci-agent-158", lastLog, threshold, now, resolved = false),
            LogAlerts.silent("a\u200Cb", "ci-agent-158", lastLog, threshold, now, resolved = false),
            LogAlerts.silent("backend", "ci-agent-9", lastLog, threshold, now, resolved = false),
        )
        assertEquals(5, enqueue(alerts).eventsCreated, "서로 다른 대상의 알림이 같은 고유 키로 겹쳐 조용히 버려졌다")
        assertEquals(alerts.map { it.eventId }.toSet(), outboxRows().map { it["event_id"] }.toSet())
        val sender = RecordingSender()
        assertEquals(5, AlertDispatcher(store, sender).dispatch(now.plusSeconds(1)).delivered)
        assertEquals(alerts.map { it.key }.toSet(), sender.sent.map { it.key }.toSet())
    }

    @Test
    fun `같은 대상의 같은 판정 시각 알림을 두 번 넣으면 한 건만 남는다 — 같은 판정의 되풀이를 고유 키로 한 번만 남기는 것이 의도다`() {
        val alert = LogAlerts.silent("backend", "ci-agent-158", null, threshold, now, resolved = false)
        assertEquals(1, enqueue(listOf(alert, alert)).eventsCreated)
        assertEquals(0, enqueue(listOf(LogAlerts.silent("backend", "ci-agent-158", null, threshold, now, resolved = false))).eventsCreated,
            "같은 판정을 다시 기록했는데 줄이 늘었다")
        assertEquals(listOf(alert.eventId), outboxRows().map { it["event_id"] })
        assertEquals(1, enqueue(listOf(LogAlerts.silent("backend", "ci-agent-158", null, threshold, now.plusSeconds(3600), resolved = false))).eventsCreated,
            "다른 판정 시각(다시 알림)이 같은 알림으로 버려졌다")
    }

    // ── 차례와 몫 ──────────────────────────────────────

    @Test
    fun `로그 알림이 한 바퀴의 50건을 먼저 차지해도 자원 알림은 다음 바퀴에 나간다 — 종류마다의 몫은 두지 않고 대상의 차례 그대로다`() {
        val logs = (1..60).map { LogAlerts.silent("svc-%02d".format(it), "ci-agent-158", now.minus(Duration.ofHours(1)), threshold, now.plusMillis(it.toLong()), resolved = false) }
        val memory = resource("memory-1", "ci-agent-164", now.plusMillis(100))
        enqueue(logs + memory)

        val first = RecordingSender()
        assertEquals(50, AlertDispatcher(store, first).dispatch(now.plusSeconds(1)).delivered)
        assertEquals(logs.take(50).map { it.eventId }, first.sent.map { it.eventId }, "첫 바퀴의 차례가 대상의 차례(머리가 오래된 순)가 아니다")

        val second = RecordingSender()
        assertEquals(11, AlertDispatcher(store, second).dispatch(now.plusSeconds(31)).delivered)
        assertEquals(logs.drop(50).map { it.eventId } + memory.eventId, second.sent.map { it.eventId }, "자원 알림이 다음 바퀴에 나가지 않았다")
    }

    @Test
    fun `로그 알림도 다시 시도는 횟수를 쌓되 횟수로 버리지 않고 받으면 지운다`() {
        enqueue(listOf(LogAlerts.silent("backend", "ci-agent-158", null, threshold, now, resolved = false)))
        val failing = RecordingSender { SendOutcome.Retry("http 503") }
        repeat(6) { AlertDispatcher(store, failing).dispatch(now.plusSeconds(30L * (it + 1))) }
        val row = outboxRows().single()
        assertEquals(6, row["attempts"])
        assertEquals("http 503", row["last_failure"])
        assertNull(row["rejected_ms"])
        assertEquals(1, AlertDispatcher(store, RecordingSender()).dispatch(now.plusSeconds(300)).delivered)
        assertEquals(emptyList<Map<String, Any?>>(), outboxRows())
    }

    // ── 섞인 판 ──────────────────────────────────────

    @Test
    fun `02a 가 먼저 배포됐다는 전제에서 — 새 판이 쓴 로그 알림 줄은 자원 규칙만 아는 옛 판의 읽기에 나오지 않고 옛 판의 건수 세기가 센다`() {
        enqueue(listOf(
            LogAlerts.silent("backend", "ci-agent-158", null, threshold, now, resolved = false),
            LogAlerts.silentAll(null, threshold, now.plusMillis(1), resolved = false),
            resource("memory-1", "ci-agent-164", now.plusMillis(2)),
        ))
        // 02a 판이 보내기에서 읽는 줄과 세는 줄 — 그 판이 아는 규칙 글자는 자원 규칙뿐이다(그 판의 SQL 과 같은 조건)
        val knownToPreviousVersion = ResourceRule.entries.joinToString(", ", "(", ")") { "'${it.wireName}'" }
        val readByPreviousVersion = jdbc.queryForList(
            "SELECT event_id FROM resource_alert_outbox WHERE rejected_ms IS NULL AND rule IN $knownToPreviousVersion ORDER BY id", String::class.java)
        assertEquals(listOf("memory-1"), readByPreviousVersion, "옛 판이 새 판의 로그 알림 줄을 읽는다(옛 판은 그 글자를 바꾸지 못해 예외를 낸다)")
        val skippedByPreviousVersion = jdbc.queryForList(
            "SELECT rule FROM resource_alert_outbox WHERE rejected_ms IS NULL AND rule NOT IN $knownToPreviousVersion ORDER BY id", String::class.java)
        assertEquals(listOf("log_silent", "log_silent_all"), skippedByPreviousVersion)
    }
}
