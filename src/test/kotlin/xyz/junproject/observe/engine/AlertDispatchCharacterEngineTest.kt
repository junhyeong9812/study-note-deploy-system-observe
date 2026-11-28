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
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.threshold.AlertCounts
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.DispatchOutcome
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 실제 PostgreSQL — 자원 알림 보내기의 지금 동작을 고정한다(특성 테스트 — 로그 알림이 같은 보낼 표를 쓰기 전에): 한 바퀴 50건 ·
 * 30초 상한에 닿아 못 보낸 줄은 손대지 않은 채 남음 · 다시 시도의 횟수 · 사유 · 시각이 쌓이고 횟수로 줄을 버리지 않음 · 보낸 줄만 지워짐 ·
 * 한 자리가 보내는 동안 다른 자리는 기다리지 않음 · 판정은 보낼 표의 남은 줄을 건드리지 않음 · 표를 거쳐도 알림이 기록한 그대로.
 * 이미 다른 엔진 테스트가 고정한 것(대상의 차례 · 디스크 묶음 · 한 대상 60건 · 두 자리가 함께 보내도 한 번씩)은 다시 쓰지 않는다.
 * 표는 전용 스키마(`dispatch_character`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlertDispatchCharacterEngineTest {
    private val schema = "dispatch_character"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val testPools = mutableListOf<HikariDataSource>()

    private fun newPool(size: Int) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@AlertDispatchCharacterEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
    })

    /** 자리 하나에 해당하는 풀 — 그 사례가 끝나면 닫힌다 */
    private fun pool(size: Int) = newPool(size).also { testPools += it }

    private val dataSource = newPool(6)
    private val jdbc = JdbcTemplate(dataSource)
    private val store = ThresholdStore(jdbc)
    private val metricStore = MetricStore(jdbc, jdbc, jdbc)
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        metricStore.createSchema()
    }

    @AfterEach fun closeTestPools() { testPools.forEach { it.close() }; testPools.clear() }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private fun alert(eventId: String, server: String, rule: ResourceRule = ResourceRule.MEMORY, targetName: String? = null,
                      severity: AlertSeverity = AlertSeverity.WARNING) =
        ResourceAlert(eventId, "resource:$server:${rule.wireName}" + (targetName?.let { ":$it" } ?: ""), rule.kind, severity,
            "$server 제목 $eventId", "자세히 $eventId", server, rule, targetName, now.plusMillis(sequence.incrementAndGet().toLong()))

    private fun enqueue(alerts: List<ResourceAlert>) = store.judgeRound(now) { JudgeResult(emptyList(), alerts) }

    /** 보낼 표의 줄 전부(id 순) — 칸 이름 → 값 */
    private fun outboxRows(): List<Map<String, Any?>> = jdbc.queryForList("SELECT * FROM resource_alert_outbox ORDER BY id")

    private fun rowOf(eventId: String): Map<String, Any?> = outboxRows().single { it["event_id"] == eventId }

    private fun targetRows(): Map<String, Long> =
        jdbc.queryForList("SELECT server, rule_group, target_name, last_attempt_ms FROM resource_alert_target")
            .associate { "${it["server"]}|${it["rule_group"]}|${it["target_name"]}" to (it["last_attempt_ms"] as Number).toLong() }

    /** 보낸 알림을 남기고, 보낼 때마다 [perSend] 만큼 [clock] 을 흘리며 [outcome] 을 돌려준다 */
    private inner class Sender(private val perSend: Duration = Duration.ZERO, private val outcome: (ResourceAlert) -> SendOutcome = { SendOutcome.Delivered }) : AlertSender {
        val sent = mutableListOf<ResourceAlert>()
        override fun send(alert: ResourceAlert): SendOutcome { synchronized(sent) { sent += alert }; clock = clock.plus(perSend); return outcome(alert) }
        fun sentIds() = synchronized(sent) { sent.map { it.eventId } }
    }

    private var clock = now

    private fun dispatcher(sender: AlertSender, queue: ThresholdStore = store) = AlertDispatcher(queue, sender, clock = { clock })

    // ── 한 바퀴의 건수 · 시간 상한 ──────────────────────

    @Test
    fun `한 바퀴는 50건까지 — 대상 55개면 오래된 50건이 가고 남은 5건은 표시 없이 남아 다음 바퀴에 간다`() {
        val alerts = (1..55).map { alert("alert-$it", "server-$it") }
        enqueue(alerts)
        val sender = Sender()
        assertEquals(DispatchOutcome(true, 50, 0, 0), dispatcher(sender).dispatch(clock))
        assertEquals((1..50).map { "alert-$it" }, sender.sentIds(), "오래된 것부터 50건이 아니다")
        val left = outboxRows()
        assertEquals((51..55).map { "alert-$it" }, left.map { it["event_id"] })
        assertTrue(left.all { it["attempts"] == 0 && it["last_failure"] == null && it["last_attempt_ms"] == null }, "보내지 않은 줄에 표시가 남았다: $left")
        assertEquals(50, targetRows().size, "보내지 않은 대상의 시도 시각이 남았다")

        clock = clock.plusSeconds(30)
        assertEquals(DispatchOutcome(true, 5, 0, 0), dispatcher(sender).dispatch(clock))
        assertEquals((51..55).map { "alert-$it" }, sender.sentIds().drop(50))
        assertEquals(emptyList<Map<String, Any?>>(), outboxRows())
    }

    @Test
    fun `30초 상한에 닿아 못 보낸 줄은 손대지 않은 채 남고 다음 바퀴에 간다`() {
        enqueue((1..5).map { alert("target-$it", "server-$it") })
        val sender = Sender(Duration.ofSeconds(10))
        assertEquals(DispatchOutcome(true, 3, 0, 0), dispatcher(sender).dispatch(clock))
        assertEquals(listOf("target-1", "target-2", "target-3"), sender.sentIds())
        val left = outboxRows()
        assertEquals(listOf("target-4", "target-5"), left.map { it["event_id"] })
        assertTrue(left.all { it["attempts"] == 0 && it["last_failure"] == null && it["last_attempt_ms"] == null }, "상한 뒤의 줄에 표시가 남았다: $left")
        assertEquals(setOf("server-1", "server-2", "server-3"), targetRows().keys.map { it.substringBefore('|') }.toSet())

        clock = clock.plusSeconds(30)
        assertEquals(DispatchOutcome(true, 2, 0, 0), dispatcher(sender).dispatch(clock))
        assertEquals(listOf("target-4", "target-5"), sender.sentIds().drop(3))
    }

    // ── 다시 시도 ──────────────────────────────────────

    @Test
    fun `다시 시도는 바퀴마다 횟수를 쌓고 마지막 사유 · 시각을 남기며 줄을 버리지 않다가 전달되면 지운다`() {
        enqueue(listOf(alert("memory-1", "ci-agent-158")))
        val reasons = ArrayDeque(listOf<() -> SendOutcome>({ SendOutcome.Retry("http 503") }, { SendOutcome.Retry("ConnectException") },
            { throw IllegalStateException("보내는 쪽 고장") }, { SendOutcome.Delivered }))
        val sender = Sender { reasons.removeFirst()() }
        val attemptTimes = mutableListOf<Instant>()
        repeat(3) { round ->
            clock = now.plusSeconds(60L * (round + 1))
            attemptTimes += clock
            assertEquals(DispatchOutcome(true, 0, 1, 0), dispatcher(sender).dispatch(clock))
        }
        val row = rowOf("memory-1")
        assertEquals(3, row["attempts"], "다시 시도의 횟수가 쌓이지 않았다")
        assertEquals("IllegalStateException", row["last_failure"], "마지막 사유가 아니다(보내는 쪽의 예외는 종류 이름)")
        assertEquals(attemptTimes.last().toEpochMilli(), (row["last_attempt_ms"] as Number).toLong())
        assertNull(row["rejected_ms"])
        assertEquals(3, store.pendingByTarget(50).single().alerts.single().attempts, "다시 시도의 줄이 읽히는 채로 남지 않았다")
        assertEquals(AlertCounts(1, 0), store.alertCounts())

        clock = now.plusSeconds(240)
        assertEquals(DispatchOutcome(true, 1, 0, 0), dispatcher(sender).dispatch(clock))
        assertEquals(emptyList<Map<String, Any?>>(), outboxRows())
        assertEquals(mapOf("ci-agent-158|memory|" to clock.toEpochMilli()), targetRows(), "전달한 시각이 대상의 마지막 시도로 남지 않았다")
        assertEquals(List(4) { "memory-1" }, sender.sentIds())
    }

    @Test
    fun `보낸 횟수가 아주 많은 줄도 버리지 않고 보낸다 — 횟수로 줄을 버리는 규칙이 없다`() {
        enqueue(listOf(alert("old-1", "ci-agent-158")))
        jdbc.update("UPDATE resource_alert_outbox SET attempts = 100000, last_failure = 'http 503', last_attempt_ms = ?", now.toEpochMilli())
        assertEquals(100000, store.pendingByTarget(50).single().alerts.single().attempts)
        val sender = Sender()
        assertEquals(DispatchOutcome(true, 1, 0, 0), dispatcher(sender).dispatch(now.plusSeconds(30)))
        assertEquals(listOf("old-1"), sender.sentIds())
        assertEquals(AlertCounts(0, 0), store.alertCounts())
    }

    // ── 지우기 ─────────────────────────────────────────

    @Test
    fun `보낸 줄만 지워진다 — 전달은 지우고 거절은 남겨 세고 다시 시도 · 차례가 오지 않은 줄은 그대로`() {
        enqueue(listOf(alert("a-1", "server-a"), alert("a-2", "server-a"), alert("b-1", "server-b"), alert("c-1", "server-c")))
        val sender = Sender { when (it.eventId) { "a-1" -> SendOutcome.Retry("http 503"); "b-1" -> SendOutcome.Rejected("http 422"); else -> SendOutcome.Delivered } }
        assertEquals(DispatchOutcome(true, 1, 1, 1), dispatcher(sender).dispatch(clock))
        assertEquals(listOf("a-1", "a-2", "b-1"), outboxRows().map { it["event_id"] }, "전달한 줄만 지워져야 한다")
        assertEquals(1 to "http 503", rowOf("a-1").let { it["attempts"] to it["last_failure"] })
        assertNull(rowOf("a-1")["rejected_ms"])
        assertEquals(listOf(0, null, null, null), rowOf("a-2").let { listOf(it["attempts"], it["last_failure"], it["last_attempt_ms"], it["rejected_ms"]) },
            "보내지 않은 줄(다시 시도 뒤의 같은 대상)에 표시가 남았다")
        assertEquals(listOf(1, "http 422"), rowOf("b-1").let { listOf(it["attempts"], it["last_failure"]) })
        assertNotNull(rowOf("b-1")["rejected_ms"], "거절이 표에 남지 않았다")
        assertEquals(AlertCounts(2, 1), store.alertCounts())
        assertEquals(listOf("a-1", "a-2"), store.pendingByTarget(50).flatMap { it.alerts }.map { it.alert.eventId }, "거절된 줄을 다시 읽었다")
    }

    // ── 한 자리만 보낸다 ────────────────────────────────

    @Test
    fun `한 자리가 보내는 동안 다른 자리는 기다리지 않고 아무것도 보내지 않으며 끝나면 그 자리도 보낼 수 있다`() {
        enqueue(listOf(alert("first", "server-1"), alert("second", "server-2")))
        val sending = CountDownLatch(1)
        val release = CountDownLatch(1)
        val blueSender = object : AlertSender {
            val sent = mutableListOf<String>()
            override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; sending.countDown(); release.await(10, TimeUnit.SECONDS); return SendOutcome.Delivered }
        }
        var blueOutcome: DispatchOutcome? = null
        val blue = thread(name = "observe-test-dispatch-blue") { blueOutcome = AlertDispatcher(store, blueSender).dispatch(now) }
        assertTrue(sending.await(10, TimeUnit.SECONDS), "파랑 자리가 보내기를 시작하지 않았다")
        val greenStore = ThresholdStore(JdbcTemplate(pool(2)))
        val greenSender = object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome = fail("잠금 없이 보냈다: ${alert.eventId}") }
        val started = System.nanoTime()
        assertEquals(DispatchOutcome(false, 0, 0, 0), AlertDispatcher(greenStore, greenSender).dispatch(now))
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(2), "보내기 잠금을 기다렸다")
        release.countDown()
        blue.join(20_000)
        assertEquals(DispatchOutcome(true, 2, 0, 0), blueOutcome)
        assertEquals(listOf("first", "second"), blueSender.sent)

        enqueue(listOf(alert("third", "server-3")))
        val greenAfter = Sender()
        assertEquals(DispatchOutcome(true, 1, 0, 0), AlertDispatcher(greenStore, greenAfter).dispatch(now.plusSeconds(30)), "잠금이 풀리지 않았다")
        assertEquals(listOf("third"), greenAfter.sentIds())
    }

    // ── 판정은 보낼 표의 남은 줄을 건드리지 않는다 ──────

    @Test
    fun `판정 한 바퀴는 상태 표를 통째로 바꿔 써도 보낼 표의 남은 줄은 그대로 둔다`() {
        enqueue(listOf(alert("left-1", "server-1"), alert("left-2", "server-2")))
        dispatcher(Sender { SendOutcome.Retry("http 503") }).dispatch(clock)
        val before = outboxRows()
        assertEquals(listOf(1, 1), before.map { it["attempts"] })

        store.judgeRound(now.plusSeconds(30)) { JudgeResult(emptyList(), emptyList()) }
        store.judgeRound(now.plusSeconds(60)) { JudgeResult(emptyList(), listOf(alert("new-1", "server-3"))) }
        ThresholdRound(metricStore, store, ThresholdJudge(), dispatcher(Sender()), store, setOf("ci-agent-164")).judgeOnce(now.plusSeconds(90))
        val after = outboxRows()
        assertEquals(before, after.filter { it["event_id"] in setOf("left-1", "left-2") }, "판정이 보낼 표의 남은 줄을 바꿨다")
        assertEquals(setOf("left-1", "left-2", "new-1", "resource:ci-agent-164:silent@${now.plusSeconds(90).toEpochMilli()}"),
            after.map { it["event_id"] }.toSet())
    }

    // ── 표를 거친 알림 ─────────────────────────────────

    @Test
    fun `실제 보낼 표를 거쳐도 보내는 쪽이 받는 알림과 본문 여섯 칸은 기록한 그대로다`() {
        val restart = alert("restart-1", "ci-agent-158", ResourceRule.RESTARTS, targetName = "front-app-green")
        val critical = alert("critical-1", "ci-agent-164", ResourceRule.DISK_CRITICAL, severity = AlertSeverity.CRITICAL)
        val resolved = alert("memory-resolved", "ci-agent-9", severity = AlertSeverity.INFO).copy(key = "resource:ci-agent-9:memory:resolved")
        enqueue(listOf(restart, critical, resolved))
        val sender = Sender()
        dispatcher(sender).dispatch(clock)
        assertEquals(listOf(restart, critical, resolved), sender.sent, "표를 거친 알림의 칸이 바뀌었다")
        assertEquals(mapOf("source" to "observe", "kind" to "resource_restarts", "severity" to "warning", "key" to "resource:ci-agent-158:restarts:front-app-green",
            "title" to "ci-agent-158 제목 restart-1", "detail" to "자세히 restart-1"), sender.sent[0].eventBody())
        assertEquals(mapOf("source" to "observe", "kind" to "resource_disk", "severity" to "critical", "key" to "resource:ci-agent-164:disk_critical",
            "title" to "ci-agent-164 제목 critical-1", "detail" to "자세히 critical-1"), sender.sent[1].eventBody())
        assertEquals("info", sender.sent[2].eventBody()["severity"])
    }
}
