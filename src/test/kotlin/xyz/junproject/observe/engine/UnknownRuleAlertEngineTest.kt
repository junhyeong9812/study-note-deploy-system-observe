package xyz.junproject.observe.engine

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.AlertCounts
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertQueue
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.AlertTarget
import xyz.junproject.observe.threshold.DispatchOutcome
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.sql.Connection
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.roundToLong

/**
 * 실제 PostgreSQL — 섞인 판(blue · green)에서 새 판이 쓴 **모르는 규칙 글자의 줄**이 보낼 표에 있을 때 옛 판의 보내기:
 * 예외로 끝나지 않고 아는 줄은 모르는 줄이 없을 때와 같은 순서로 전부 나간다 · 모르는 줄은 지워지지도 표시(횟수 · 사유 · 시도 시각 · 거절)되지도 않고
 * 대상의 시도 시각도 남기지 않는다(다음 판이 처음 보내는 줄처럼 읽는다) · 규칙 글자마다의 건수가 보낼 표의 건수([AlertCounts.unknownRules]) ·
 * 헬스의 `skipped_unknown_rule_alerts` · 보내기 바퀴마다 WARN 한 줄에 드러난다(바퀴 끝, 보내기 잠금 밖에서 한 번 센 값) ·
 * 수천 건이어도 아는 줄이 굶지 않고 한 바퀴의 건수 · 대상의 차례를 먹지 않는다 · 모르는 규칙의 글자가 아는 묶음의 이름(`disk`)과 같아도
 * 같은 묶음의 아는 줄이 굶지 않는다(모르는 줄은 대상마다의 머리를 고르기 전에 SQL 에서 걸러진다) · 건수 세기가 실패해도 아는 줄은
 * 실제 표에서 전부 보내지고 지워진다(헬스는 "세지 못함" — null) · 건수 세기는 보내기 잠금을 쥐지 않는다(세는 동안 다른 연결이 보내기 잠금을 얻는다) ·
 * 건수 세기의 조회는 시간 상한(기본 2초, `ThresholdStore(countTimeout = …)`)에서 끊긴다 · 판정 한 바퀴가 모르는 줄을 지우지 않는다.
 * 모르는 규칙 글자: 다음 판의 `log_silent` · `log_silent_all`, 엉뚱한 글자, 빈 글자, 대소문자만 다른 글자(`MEMORY` — 글자는 그대로 맞춰 본다).
 * 모르는 줄은 새 판이 쓸 꼴을 흉내 내 SQL 로 직접 넣는다. 표는 전용 스키마(`unknown_rule`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UnknownRuleAlertEngineTest {
    private val schema = "unknown_rule"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@UnknownRuleAlertEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 6
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = ThresholdStore(jdbc)
    private val metricStore = MetricStore(jdbc, jdbc, jdbc)
    private val now = Instant.parse("2026-11-25T03:00:00Z")
    private val sequence = AtomicInteger()
    private val unknownRules = listOf("log_silent", "log_silent_all", "zzz-엉뚱한-글자", "", "MEMORY")

    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        metricStore.createSchema()
        appender.list.clear()
    }

    @AfterEach fun clearLog() = appender.list.clear()

    @AfterAll
    fun close() {
        (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender)
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    // ── 줄 만들기 ─────────────────────────────────────

    private fun at(offsetMillis: Long) = now.plusMillis(offsetMillis)

    private fun known(eventId: String, server: String, createdAt: Instant, rule: ResourceRule = ResourceRule.MEMORY, targetName: String? = null) =
        ResourceAlert(eventId, "resource:$server:${rule.wireName}" + (targetName?.let { ":$it" } ?: ""), rule.kind, AlertSeverity.WARNING,
            "$server 제목 $eventId", "자세히", server, rule, targetName, createdAt)

    private fun enqueueKnown(alerts: List<ResourceAlert>) = store.judgeRound(now) { JudgeResult(emptyList(), alerts) }

    /** 다음 판이 쓸 꼴의 줄 — 이 판이 모르는 [rule] 글자. 서비스 이름은 대상 이름 칸에 */
    private fun insertUnknown(eventId: String, rule: String, createdAt: Instant, server: String = "ci-agent-158", targetName: String? = "backend") {
        jdbc.update(
            """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            eventId, "log:$server:$rule:${targetName ?: ""}", "log_silent", "warning", "$server $targetName 로그 끊김", "자세히", server, rule, targetName,
            createdAt.toEpochMilli(),
        )
    }

    private fun outboxRows(): List<Map<String, Any?>> = jdbc.queryForList("SELECT * FROM resource_alert_outbox ORDER BY id")

    private fun unknownRows() = outboxRows().filter { it["rule"] as String !in ResourceRule.entries.map { rule -> rule.wireName } }

    private fun targetGroups(): Set<String> = jdbc.queryForList("SELECT rule_group FROM resource_alert_target", String::class.java).filterNotNull().toSet()

    private class RecordingSender(private val outcome: (ResourceAlert) -> SendOutcome = { SendOutcome.Delivered }) : AlertSender {
        val sent = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; return outcome(alert) }
    }

    /** 아는 줄 넷 — 두 건이 한 대상(ci-agent-158 메모리), 나머지는 대상이 다르다 */
    private fun knownSet() = listOf(
        known("memory-1", "ci-agent-158", at(10)),
        known("memory-2", "ci-agent-158", at(20)),
        known("cpu-1", "ci-agent-164", at(30), ResourceRule.CPU),
        known("restart-1", "ci-agent-9", at(40), ResourceRule.RESTARTS, "front-app-green"),
    )

    // ── 예외 없이 아는 줄은 그대로 나간다 ────────────────

    @Test
    fun `모르는 규칙의 줄이 앞 · 뒤 · 사이 어디에 있어도 보내기는 예외 없이 아는 줄을 모르는 줄이 없을 때와 같은 순서로 전부 보낸다`() {
        enqueueKnown(knownSet())
        val baselineSender = RecordingSender()
        assertEquals(DispatchOutcome(true, 4, 0, 0), AlertDispatcher(store, baselineSender).dispatch(now.plusSeconds(1)))
        val baseline = baselineSender.sent

        // 모르는 줄의 자리: 아는 줄보다 모두 앞 · 모두 뒤 · 아는 줄 사이사이 · 아는 줄과 같은 시각 · 아는 줄과 같은 서버 · 대상 이름
        val arrangements: Map<String, List<Pair<Long, Pair<String, String?>>>> = mapOf(
            "앞" to unknownRules.mapIndexed { index, _ -> index.toLong() to ("ci-agent-158" to "backend") },
            "뒤" to unknownRules.mapIndexed { index, _ -> 100L + index to ("ci-agent-158" to "backend") },
            "사이" to unknownRules.mapIndexed { index, _ -> 15L + 10 * index to ("ci-agent-164" to "svc-$index") },
            "같은 시각" to unknownRules.mapIndexed { index, _ -> 10L * (index % 4 + 1) to ("ci-agent-9" to "front-app-green") },
            "같은 서버 · 대상 이름 없음" to unknownRules.mapIndexed { index, _ -> 5L + 10 * index to ("ci-agent-158" to null) },
        )
        for ((name, placement) in arrangements) {
            jdbc.execute("TRUNCATE resource_alert_outbox, resource_alert_target")
            unknownRules.forEachIndexed { index, rule ->
                val (offset, where) = placement[index]
                insertUnknown("unknown-$name-$index", rule, at(offset), server = where.first, targetName = where.second)
            }
            enqueueKnown(knownSet())
            val sender = RecordingSender()
            val outcome = assertDoesNotThrow<DispatchOutcome>({ AlertDispatcher(store, sender).dispatch(now.plusSeconds(1)) }, "$name: 모르는 줄에서 보내기가 예외로 끝났다")
            assertEquals(listOf(true, 4, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected), "$name: $outcome")
            assertEquals(baseline, sender.sent, "$name: 아는 줄을 다 보내지 않았거나 순서가 모르는 줄이 없을 때와 다르다")
        }
    }

    @Test
    fun `보내기 한 바퀴(예약이 부르는 길)가 모르는 줄 때문에 실패로 남지 않는다`() {
        unknownRules.forEachIndexed { index, rule -> insertUnknown("unknown-$index", rule, at(index.toLong())) }
        enqueueKnown(knownSet())
        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), store, emptySet())
        val outcome = round.dispatchOnce(now.plusSeconds(1))
        assertNull(round.lastDispatchFailure, "보내기 바퀴가 실패로 남았다")
        assertNotNull(outcome, "보내기 바퀴가 실패로 남았다")
        assertEquals(4, outcome!!.delivered)
        assertEquals(setOf("memory-1", "memory-2", "cpu-1", "restart-1"), sender.sent.toSet())
    }

    // ── 모르는 줄은 그대로 남는다 ──────────────────────

    @Test
    fun `모르는 줄은 몇 바퀴가 지나도 지워지지도 표시되지도 않고 대상의 시도 시각도 남기지 않는다`() {
        unknownRules.forEachIndexed { index, rule -> insertUnknown("unknown-$index", rule, at(index.toLong()), server = "server-$index") }
        enqueueKnown(knownSet())
        val before = unknownRows()
        assertEquals(unknownRules.size, before.size)
        // 아는 줄이 전달 · 다시 시도 · 거절로 갈리는 바퀴를 셋 — 모르는 줄은 어느 쪽에도 끼지 않는다
        val sender = RecordingSender { when (it.eventId) { "memory-1" -> SendOutcome.Retry("http 503"); "cpu-1" -> SendOutcome.Rejected("http 422"); else -> SendOutcome.Delivered } }
        repeat(3) { round -> AlertDispatcher(store, sender).dispatch(now.plusSeconds(30L * (round + 1))) }
        assertEquals(before, unknownRows(), "모르는 줄이 지워졌거나 횟수 · 사유 · 시도 시각 · 거절이 남았다")
        assertTrue(unknownRows().all { it["attempts"] == 0 && it["last_failure"] == null && it["last_attempt_ms"] == null && it["rejected_ms"] == null })
        assertFalse(sender.sent.any { it.startsWith("unknown") }, "모르는 줄을 보냈다: ${sender.sent}")
        assertEquals(setOf("memory", "cpu", "restarts"), targetGroups(), "모르는 규칙의 대상에 시도 시각이 남았다")
    }

    // ── 건너뛴 수가 드러난다 ───────────────────────────

    @Test
    fun `건너뛴 모르는 줄의 수가 규칙 글자마다 보낼 표의 건수에 드러나고 보내기 뒤에도 그대로다`() {
        val counts = mapOf("log_silent" to 3L, "log_silent_all" to 1L, "" to 1L, "zzz-엉뚱한-글자" to 2L)
        var offset = 0L
        for ((rule, count) in counts) repeat(count.toInt()) { insertUnknown("unknown-$rule-$it", rule, at(offset++), targetName = "svc-$it") }
        enqueueKnown(listOf(known("memory-1", "ci-agent-158", at(100)), known("cpu-1", "ci-agent-164", at(110), ResourceRule.CPU)))
        assertEquals(AlertCounts(9, 0, counts), store.alertCounts(), "못 보낸 수(모르는 줄 포함) · 규칙 글자마다의 모르는 줄 수가 다르다")

        val outcome = AlertDispatcher(store, RecordingSender()).dispatch(now.plusSeconds(1))
        assertEquals(listOf(true, 2, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(AlertCounts(7, 0, counts), store.alertCounts())
        val next = AlertDispatcher(store, RecordingSender()).dispatch(now.plusSeconds(31))
        assertEquals(listOf(true, 0, 0, 0), listOf(next.lockAcquired, next.delivered, next.retrying, next.rejected))
        assertEquals(AlertCounts(7, 0, counts), store.alertCounts(), "다음 바퀴 뒤에 모르는 줄의 수가 바뀌었다")
    }

    @Test
    fun `모르는 줄이 없으면 보낼 표의 모르는 줄 맵은 비어 있다`() {
        enqueueKnown(knownSet())
        assertEquals(AlertCounts(4, 0, emptyMap()), store.alertCounts())
        val outcome = AlertDispatcher(store, RecordingSender()).dispatch(now.plusSeconds(1))
        assertEquals(listOf(true, 4, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(AlertCounts(0, 0, emptyMap()), store.alertCounts())
    }

    private fun health(round: ThresholdRound): Map<String, Any?> {
        val collector = MetricCollector(FakeMetricStream(), FakeMetricSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        return MetricHealth(collector, { 0L }, { 0L }, { null }, { emptyMap() }, { null }, { null }, { null }, startedAt = now, thresholds = round).report(now)
    }

    private fun unknownRuleWarnings() = appender.list.filter { it.level == Level.WARN && it.formattedMessage.contains("unknown rule") }

    @Test
    fun `헬스의 지표 칸에 건너뛴 모르는 줄의 수가 나오고 보내기 한 바퀴마다 WARN 한 줄에 규칙 글자와 수가 남는다`() {
        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), store, emptySet())
        val before = health(round)
        assertTrue(before.containsKey("skipped_unknown_rule_alerts"), "헬스에 skipped_unknown_rule_alerts 칸이 없다: $before")
        assertNull(before["skipped_unknown_rule_alerts"], "아직 세지 않았는데 수가 나왔다")

        repeat(4) { insertUnknown("silent-$it", "log_silent", at(it.toLong()), targetName = "svc-$it") }
        repeat(2) { insertUnknown("silent-all-$it", "log_silent_all", at(10L + it), targetName = null) }
        insertUnknown("odd-0", "zzz-엉뚱한-글자", at(20))
        enqueueKnown(listOf(known("memory-1", "ci-agent-158", at(30))))
        round.dispatchOnce(now.plusSeconds(1))
        round.dispatchOnce(now.plusSeconds(31))
        assertEquals(listOf("memory-1"), sender.sent)

        val body = health(round)
        assertEquals(7L, (body["skipped_unknown_rule_alerts"] as? Number)?.toLong(), "헬스에 건너뛴 모르는 줄의 수가 없다: $body")
        assertEquals(7L, (body["undelivered_alerts"] as Number).toLong(), "못 보낸 수에 모르는 줄이 들지 않았다")
        assertEquals("", body["last_dispatch_failure"])
        assertEquals("ok", body["status"], "모르는 줄이 지표 수집의 판정을 바꿨다")

        val warnings = unknownRuleWarnings()
        assertEquals(2, warnings.size, "보내기 한 바퀴에 WARN 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        for (warning in warnings) {
            for (part in listOf("log_silent=4", "log_silent_all=2", "zzz-엉뚱한-글자=1", "total 7")) {
                assertTrue(warning.formattedMessage.contains(part), "WARN 에 '$part' 가 없다: ${warning.formattedMessage}")
            }
        }
    }

    // ── 굶지 않는다 · 차례를 먹지 않는다 ─────────────────

    @Test
    fun `모르는 줄이 수천 건 먼저 쌓여 있어도 아는 줄은 첫 바퀴에 전부 나간다`() {
        // 한 대상에 2000건, 500 대상에 2건씩 — 모두 아는 줄보다 오래됐다
        jdbc.update(
            """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               SELECT 'bulk-one-' || n, 'log:ci-agent-158:log_silent:backend', 'log_silent', 'warning', 't', 'd', 'ci-agent-158', 'log_silent', 'backend', ? + n
               FROM generate_series(1, 2000) AS n""", now.toEpochMilli())
        jdbc.update(
            """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               SELECT 'bulk-many-' || n, 'log:ci-agent-164:log_silent:svc-' || (n % 500), 'log_silent', 'warning', 't', 'd', 'ci-agent-164', 'log_silent',
                      'svc-' || (n % 500), ? + n
               FROM generate_series(1, 1000) AS n""", now.toEpochMilli())
        val knownAlerts = (1..12).map { known("known-$it", "server-$it", at(10_000L + it)) }
        enqueueKnown(knownAlerts)
        val sender = RecordingSender()
        val outcome = AlertDispatcher(store, sender).dispatch(now.plusSeconds(20))
        assertEquals(knownAlerts.map { it.eventId }, sender.sent, "모르는 줄 뒤에서 아는 줄이 굶었다")
        assertEquals(listOf(true, 12, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(AlertCounts(3000, 0, mapOf("log_silent" to 3000L)), store.alertCounts())
        assertEquals(3000, unknownRows().size)
    }

    @Test
    fun `모르는 줄은 한 바퀴의 건수와 대상의 차례를 먹지 않는다`() {
        // 모르는 대상 다섯의 머리가 아는 대상들보다 오래됐다 — 차례에 끼면 묶음 크기 3 을 먼저 먹는다
        (1..5).forEach { insertUnknown("unknown-$it", "log_silent", at(it.toLong()), server = "server-$it", targetName = "svc-$it") }
        enqueueKnown(listOf(known("a-1", "server-a", at(100)), known("b-1", "server-b", at(110)), known("c-1", "server-c", at(120))))
        val sender = RecordingSender()
        assertEquals(3, AlertDispatcher(store, sender, batchSize = 3, clock = { now.plusSeconds(1) }).dispatch(now.plusSeconds(1)).delivered)
        assertEquals(listOf("a-1", "b-1", "c-1"), sender.sent, "모르는 줄이 한 바퀴의 건수를 먹었다")

        // 다음 바퀴: 시도한 적 없는 아는 대상(d)이 먼저, 그다음 마지막 시도가 오래된 아는 대상 — 모르는 대상은 차례에 없다
        enqueueKnown(listOf(known("a-2", "server-a", at(200)), known("d-1", "server-d", at(210))))
        var clock = now.plusSeconds(31)
        val timed = RecordingSender { clock = clock.plusSeconds(10); SendOutcome.Delivered }
        AlertDispatcher(store, timed, batchSize = 3, clock = { clock }).dispatch(clock)
        assertEquals(listOf("d-1", "a-2"), timed.sent)
        assertEquals(setOf("memory"), targetGroups())
        assertEquals(5, unknownRows().size)
    }

    @Test
    fun `모르는 규칙의 글자가 아는 묶음의 이름(disk)과 같은 줄이 한 대상에 50건 넘게 먼저 있어도 같은 대상의 아는 디스크 줄은 그 바퀴에 나간다`() {
        // 아는 디스크 규칙의 묶음 이름과 같은 글자 — 대상(서버 · 묶음 · 대상 이름)이 아는 디스크 줄과 같아진다
        jdbc.update(
            """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
               SELECT 'disk-named-' || n, 'log:ci-agent-158:disk', 'log_silent', 'warning', 't', 'd', 'ci-agent-158', 'disk', NULL, ? + n
               FROM generate_series(1, 120) AS n""", now.toEpochMilli())
        enqueueKnown(listOf(known("disk-warning-1", "ci-agent-158", at(10_000), ResourceRule.DISK_WARNING)))

        val heads = store.pendingByTarget(50)
        assertEquals(listOf(AlertTarget("ci-agent-158", "disk", null)), heads.map { it.target }, "대상의 머리에 아는 디스크 줄의 대상이 없다")
        assertEquals(listOf("disk-warning-1"), heads.single().alerts.map { it.alert.eventId }, "모르는 줄이 대상의 머리를 먹었다")

        val sender = RecordingSender()
        val outcome = AlertDispatcher(store, sender).dispatch(now.plusSeconds(20))
        assertEquals(listOf("disk-warning-1"), sender.sent, "같은 묶음 이름의 모르는 줄 뒤에서 아는 디스크 줄이 굶었다")
        assertEquals(listOf(true, 1, 0, 0), listOf(outcome.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected))
        assertEquals(AlertCounts(120, 0, mapOf("disk" to 120L)), store.alertCounts())
        assertEquals(120, unknownRows().size)
        assertTrue(unknownRows().all { it["attempts"] == 0 && it["last_attempt_ms"] == null && it["rejected_ms"] == null }, "모르는 줄이 표시됐다")
    }

    @Test
    fun `건수 세기가 실패해도 실제 표의 아는 줄은 그 바퀴에 전부 보내지고 지워지며 보내기 바퀴는 실패로 남지 않는다`() {
        unknownRules.forEachIndexed { index, rule -> insertUnknown("unknown-$index", rule, at(index.toLong())) }
        enqueueKnown(knownSet())
        val countFails = object : AlertQueue by store {
            override fun alertCounts(): AlertCounts = throw QueryTimeoutException("canceling statement due to statement timeout")
        }
        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), countFails, emptySet())
        val outcome = round.dispatchOnce(now.plusSeconds(1))
        assertNull(round.lastDispatchFailure, "세기의 실패가 보내기 실패로 남았다")
        assertNotNull(outcome, "세기의 실패가 보내기 바퀴를 실패로 만들었다")
        assertEquals(listOf(true, 4, 0, 0), listOf(outcome!!.lockAcquired, outcome.delivered, outcome.retrying, outcome.rejected), "$outcome")
        assertEquals(setOf("memory-1", "memory-2", "cpu-1", "restart-1"), sender.sent.toSet())
        assertEquals(unknownRules.size, outboxRows().size, "보낸 아는 줄이 지워지지 않았거나 모르는 줄이 지워졌다")
        assertEquals(unknownRules.size, unknownRows().size)
        assertNull(health(round)["skipped_unknown_rule_alerts"], "세지 못했는데 헬스에 수가 나왔다")
        val warnings = unknownRuleWarnings()
        assertEquals(1, warnings.size, "세지 못한 바퀴의 WARN 이 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        assertTrue(warnings.single().formattedMessage.contains("QueryTimeoutException"))
        assertFalse(warnings.single().formattedMessage.contains("statement timeout"), "WARN 에 예외의 문구가 실렸다")
    }

    // ── 건수 세기는 보내기 잠금을 쥐지 않고 시간 상한에서 끊긴다 ─────────

    private fun pool(size: Int) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@UnknownRuleAlertEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
    })

    @Test
    fun `건수 세기가 도는 동안 다른 연결이 보내기 잠금을 얻을 수 있다`() {
        unknownRules.forEachIndexed { index, rule -> insertUnknown("unknown-$index", rule, at(index.toLong())) }
        enqueueKnown(knownSet())
        pool(2).use { otherPool ->
            val other = ThresholdStore(JdbcTemplate(otherPool)).also { it.prepareSchema() }
            var otherGotLock: Boolean? = null
            val counting = object : AlertQueue by store {
                override fun alertCounts(): AlertCounts {
                    otherGotLock = other.whileHoldingDispatchLock { true } ?: false
                    return store.alertCounts()
                }
            }
            val sender = RecordingSender()
            val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), counting, emptySet())
            round.dispatchOnce(now.plusSeconds(1))
            assertEquals(true, otherGotLock, "건수 세기가 보내기 잠금을 쥔 채로 돌았다(또는 세지 않았다)")
            assertEquals(4, sender.sent.size)
            assertEquals(unknownRules.size.toLong(), (health(round)["skipped_unknown_rule_alerts"] as? Number)?.toLong())
        }
    }

    /** 다른 연결에서 보낼 표를 잠근다(모든 읽기가 기다린다) — 돌려받은 연결을 [release] 할 때까지 */
    private fun lockOutbox(): Connection = administration.connection.also { connection ->
        connection.autoCommit = false
        connection.createStatement().use { it.execute("LOCK TABLE $schema.resource_alert_outbox IN ACCESS EXCLUSIVE MODE") }
    }

    private fun release(connection: Connection) {
        try { connection.rollback() } finally { connection.autoCommit = true; connection.close() }
    }

    /**
     * 건수 세기가 [countingStore] 로 세기 직전에 보낼 표를 다른 연결에서 잠가 세기를 붙잡은 채 보내기 바퀴 하나 — 걸린 시간과 바퀴.
     * 보내기는 잠그기 전에 끝난다(보내기는 [store], 세기만 잠긴다). 상한이 없으면 15초에서 실패하고 잠금을 풀어 준다
     */
    private fun dispatchWithCountingStuck(countingStore: ThresholdStore): Pair<Duration, ThresholdRound> {
        countingStore.prepareSchema()
        val holders = mutableListOf<Connection>()
        val counting = object : AlertQueue by countingStore {
            override fun alertCounts(): AlertCounts { holders += lockOutbox(); return countingStore.alertCounts() }
        }
        val sender = RecordingSender()
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), counting, emptySet())
        try {
            val started = System.nanoTime()
            val outcome = CompletableFuture.supplyAsync { round.dispatchOnce(now.plusSeconds(1)) }.get(15, TimeUnit.SECONDS)
            val elapsed = Duration.ofNanos(System.nanoTime() - started)
            assertEquals(listOf(true, 1, 0, 0), outcome?.let { listOf(it.lockAcquired, it.delivered, it.retrying, it.rejected) }, "세기가 붙잡혀 보내기 결과가 바뀌었다")
            assertNull(round.lastDispatchFailure, "세기의 실패가 보내기 실패로 남았다")
            assertEquals(listOf("memory-1"), sender.sent)
            return elapsed to round
        } finally {
            holders.forEach(::release)
        }
    }

    private fun assertCountedAsFailed(round: ThresholdRound) {
        val body = health(round)
        assertTrue(body.containsKey("skipped_unknown_rule_alerts"))
        assertNull(body["skipped_unknown_rule_alerts"], "세기가 시간 상한에서 끊겼는데 헬스에 수가 나왔다")
        val warnings = unknownRuleWarnings()
        assertEquals(1, warnings.size, "세지 못한 바퀴의 WARN 이 한 줄이 아니다: ${appender.list.map { "${it.level} ${it.formattedMessage}" }}")
        assertNull(warnings.single().throwableProxy, "WARN 에 예외가 붙었다")
        assertFalse(warnings.single().formattedMessage.contains("canceling statement"), "WARN 에 예외의 문구가 실렸다: ${warnings.single().formattedMessage}")
    }

    @Test
    fun `건수 세기의 조회는 주입한 시간 상한에서 끊겨 그 바퀴는 세지 못함으로 끝난다`() {
        insertUnknown("unknown-0", "log_silent", at(0))
        enqueueKnown(listOf(known("memory-1", "ci-agent-158", at(10))))
        val (elapsed, round) = dispatchWithCountingStuck(ThresholdStore(jdbc, countTimeout = Duration.ofSeconds(1)))
        assertTrue(elapsed >= Duration.ofMillis(800), "세기가 상한보다 일찍 끝났다 — 잠금을 기다리지 않았다(${elapsed.toMillis()}ms)")
        assertTrue(elapsed < Duration.ofMillis(1_900), "세기가 1초 상한에서 끊기지 않았다(${elapsed.toMillis()}ms)")
        assertCountedAsFailed(round)
    }

    @Test
    fun `건수 세기의 조회는 기본 2초 상한에서 끊긴다`() {
        insertUnknown("unknown-0", "log_silent", at(0))
        enqueueKnown(listOf(known("memory-1", "ci-agent-158", at(10))))
        val (elapsed, round) = dispatchWithCountingStuck(ThresholdStore(jdbc))
        assertTrue(elapsed >= Duration.ofMillis(1_500), "기본 상한이 2초보다 짧다(${elapsed.toMillis()}ms)")
        assertTrue(elapsed < Duration.ofMillis(4_000), "세기가 2초 상한에서 끊기지 않았다(${elapsed.toMillis()}ms)")
        assertCountedAsFailed(round)
    }

    // ── 세기의 시간 상한은 그 조회에만 — 풀의 연결에 남지 않는다 ─────────

    private val countPoolSize = 3

    /** 풀의 연결을 모두 한꺼번에 빌려(같은 연결을 두 번 보지 않게) 연결마다 `SHOW statement_timeout` */
    private fun statementTimeoutsOfEveryConnection(countPool: HikariDataSource): List<String> {
        val borrowed = (1..countPoolSize).map { countPool.connection }
        try {
            return borrowed.map { connection -> connection.createStatement().use { statement -> statement.executeQuery("SHOW statement_timeout").use { it.next(); it.getString(1) } } }
        } finally {
            borrowed.forEach { it.close() }
        }
    }

    @Test
    fun `건수 세기가 끝난 뒤 그 풀의 어느 연결에도 세기의 시간 상한이 남지 않는다`() {
        insertUnknown("unknown-0", "log_silent", at(0))
        enqueueKnown(listOf(known("memory-1", "ci-agent-158", at(10))))
        pool(countPoolSize).use { countPool ->
            val counting = ThresholdStore(JdbcTemplate(countPool), countTimeout = Duration.ofSeconds(1)).also { it.prepareSchema() }
            val before = statementTimeoutsOfEveryConnection(countPool)
            assertEquals(countPoolSize, before.size)
            repeat(countPoolSize * 2) { assertEquals(AlertCounts(2, 0, mapOf("log_silent" to 1L)), counting.alertCounts()) }
            assertEquals(before, statementTimeoutsOfEveryConnection(countPool), "세기의 시간 상한이 풀의 연결에 남았다(같은 풀의 다른 문장도 끊긴다)")
            // 남은 상한이 실제로 다른 문장을 끊지 않는다 — 상한(1초)보다 오래 걸리는 문장이 같은 풀에서 끝난다
            assertDoesNotThrow { JdbcTemplate(countPool).execute("SELECT pg_sleep(1.5)") }
        }
    }

    @Test
    fun `건수 세기가 시간 상한에서 끊긴 뒤에도 그 풀의 어느 연결에도 세기의 시간 상한이 남지 않는다`() {
        insertUnknown("unknown-0", "log_silent", at(0))
        pool(countPoolSize).use { countPool ->
            val counting = ThresholdStore(JdbcTemplate(countPool), countTimeout = Duration.ofSeconds(1)).also { it.prepareSchema() }
            val before = statementTimeoutsOfEveryConnection(countPool)
            val holder = lockOutbox()
            try {
                val failure = CompletableFuture.supplyAsync { runCatching { counting.alertCounts() } }.get(15, TimeUnit.SECONDS).exceptionOrNull()
                assertNotNull(failure, "표를 잠갔는데 세기가 시간 상한에서 끊기지 않았다")
            } finally {
                release(holder)
            }
            assertEquals(before, statementTimeoutsOfEveryConnection(countPool), "시간 초과로 끝난 세기의 상한이 풀의 연결에 남았다")
            assertDoesNotThrow { JdbcTemplate(countPool).execute("SELECT pg_sleep(1.5)") }
        }
    }

    // ── 판정은 모르는 줄을 지우지 않는다 ─────────────────

    private fun highMemory(server: String, endingAt: Instant) = (0 until 10).map {
        val measuredAt = endingAt.minusSeconds(30L * it)
        ResourceSample(StreamId(measuredAt.toEpochMilli(), sequence.incrementAndGet()), server, measuredAt, measuredAt, 30_000,
            HostValues(cpuPct = 5.0, load1 = 0.1, memTotal = 16_000_000_000, memUsed = (95.0 * 160_000_000).roundToLong(), diskTotal = 1_000, diskUsed = 100),
            emptyList(), emptyList(), emptyList())
    }

    @Test
    fun `판정 한 바퀴가 상태 표를 통째로 바꿔 쓰고 알림을 더해도 모르는 줄은 그대로 남는다`() {
        unknownRules.forEachIndexed { index, rule -> insertUnknown("unknown-$index", rule, at(index.toLong())) }
        val before = unknownRows()
        metricStore.insertAll(highMemory("ci-agent-158", now.plusSeconds(60)))
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, RecordingSender()), store, setOf("ci-agent-164"))
        val judged = round.judgeOnce(now.plusSeconds(60))
        assertTrue(judged.judged, "판정하지 못했다: ${judged.judgeFailure}")
        assertEquals(2, judged.eventsCreated, "메모리 넘음 · 끊김 알림이 생기지 않았다")
        round.judgeOnce(now.plusSeconds(90))
        store.judgeRound(now.plusSeconds(120)) { JudgeResult(emptyList(), emptyList()) }
        assertEquals(before, unknownRows(), "판정이 모르는 줄을 지웠거나 바꿨다")
    }
}
