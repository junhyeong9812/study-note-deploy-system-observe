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
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.GpuValues
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.AlertCounts
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.DispatchOutcome
import xyz.junproject.observe.threshold.FiringRule
import xyz.junproject.observe.threshold.JudgeResult
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RoundOutcome
import xyz.junproject.observe.threshold.RuleState
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.math.roundToLong

/**
 * 실제 PostgreSQL — 명세 §2.4 · §11.4, ⑤ 자동(observe): 판정 상태를 DB 에 두어 재시작 뒤 같은 알림을 다시 만들지 않음 ·
 * 상태와 보낼 알림을 한 트랜잭션에(중간 실패 주입) · 한 번에 한 자리만 판정(DB 잠금 — 두 저장소 · 두 스레드) · 같은 고유 키는 한 건 ·
 * 못 보낸 알림은 남아 다음 바퀴에 다시 · 보낸 것은 다시 보내지 않음 · 두 자리가 함께 보내도 한 번씩 · 실제 표본으로 도는 한 바퀴.
 * 시각은 인자로 정한다. 표는 전용 스키마(`threshold_engine`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ThresholdStoreEngineTest {
    private val schema = "threshold_engine"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val openedPools = mutableListOf<HikariDataSource>()

    /** 자리 하나에 해당하는 연결 풀 — 저장소 둘을 서로 다른 풀로 만들어 두 인스턴스를 흉내 낸다 */
    private fun pool(size: Int = 4) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@ThresholdStoreEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = size
    }).also { openedPools += it }

    private val dataSource = pool(8)
    private val store = ThresholdStore(JdbcTemplate(dataSource))
    private val metricStore = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val server = "ci-agent-158"
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        metricStore.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        openedPools.forEach { it.close() }
        administration.close()
    }

    private fun state(rule: ResourceRule = ResourceRule.MEMORY, since: Instant = now, firing: Boolean = true, target: String = "", targetName: String? = null,
                      value: Double? = 91.5, server: String = this.server) =
        RuleState(server, rule, target, targetName, firing, if (firing) since else null, since, value)

    private fun alert(eventId: String, key: String = "resource:$server:memory", rule: ResourceRule = ResourceRule.MEMORY, createdAt: Instant = now,
                      severity: AlertSeverity = AlertSeverity.WARNING, targetName: String? = null) =
        ResourceAlert(eventId, key, rule.kind, severity, "$server 제목 $eventId", "자세히", server, rule, targetName, createdAt)

    /** 앞 상태에 메모리 넘음이 없으면 넘음으로 바꾸고 알림 하나를 만드는 판정 — 저장소의 동작만 보려고 규칙은 단순하게 */
    private fun firstFiring(at: Instant = now): (List<RuleState>) -> JudgeResult = { previous ->
        if (previous.any { it.rule == ResourceRule.MEMORY && it.firing }) JudgeResult(previous, emptyList())
        else JudgeResult(previous.filterNot { it.rule == ResourceRule.MEMORY } + state(since = at), listOf(alert("resource:$server:memory@${at.toEpochMilli()}", createdAt = at)))
    }

    // ── 상태 · 알림의 기록 ────────────────────────────────

    @Test
    fun `빈 표에서 상태도 알림도 넘어 있는 것도 없고 스키마를 두 번 만들어도 그대로다`() {
        store.createSchema()
        assertEquals(emptyList<RuleState>(), store.states())
        assertEquals(emptyList<Any>(), store.pendingAlerts(10))
        assertEquals(AlertCounts(0, 0), store.alertCounts())
        assertEquals(emptyList<FiringRule>(), store.firing())
    }

    @Test
    fun `한 바퀴가 상태와 알림을 기록하고 값이 그대로 돌아온다`() {
        val states = listOf(
            state(ResourceRule.MEMORY),
            state(ResourceRule.RESTARTS, target = "0123456789ab", targetName = "front-app-green", value = 4.0),
            state(ResourceRule.CPU, firing = false, value = null),
            state(ResourceRule.DISK_CRITICAL, server = "ci-agent-164", value = 96.25),
        )
        val alerts = listOf(alert("e-1"), alert("e-2", key = "resource:ci-agent-164:disk_critical", rule = ResourceRule.DISK_CRITICAL, severity = AlertSeverity.CRITICAL),
            alert("e-3", key = "resource:$server:restarts:front-app-green", rule = ResourceRule.RESTARTS, targetName = "front-app-green"))
        assertEquals(RoundOutcome(true, 3), store.judgeRound(now) { previous -> assertEquals(emptyList<RuleState>(), previous); JudgeResult(states, alerts) })
        assertEquals(states.toSet(), store.states().toSet())
        assertEquals(alerts, store.pendingAlerts(10).map { it.alert }, "알림이 바뀌었거나 순서(오래된 것부터 — 같은 시각이면 기록 순)가 다르다")
        assertTrue(store.pendingAlerts(10).all { it.attempts == 0 && it.lastFailure == null })
        assertEquals(AlertCounts(3, 0), store.alertCounts())
        assertEquals(listOf(
            FiringRule("ci-agent-158", "memory", null, "warning", now, 91.5),
            FiringRule("ci-agent-158", "restarts", "front-app-green", "warning", now, 4.0),
            FiringRule("ci-agent-164", "disk_critical", null, "critical", now, 96.25),
        ), store.firing(), "넘어 있지 않은 상태가 들었거나 칸이 다르다")
    }

    @Test
    fun `다음 바퀴의 판정은 저장된 앞 상태를 받아 같은 상태가 이어지면 알림을 다시 만들지 않는다`() {
        assertEquals(RoundOutcome(true, 1), store.judgeRound(now, firstFiring()))
        assertEquals(RoundOutcome(true, 0), store.judgeRound(now.plusSeconds(30), firstFiring(now.plusSeconds(30))))
        assertEquals(1, store.pendingAlerts(10).size)
    }

    @Test
    fun `관측 서버가 재시작해도(새 저장소 · 새 연결 풀) 상태가 남아 같은 알림을 다시 만들지 않는다`() {
        store.judgeRound(now, firstFiring())
        val restarted = ThresholdStore(JdbcTemplate(pool(2)))
        assertEquals(RoundOutcome(true, 0), restarted.judgeRound(now.plusSeconds(30), firstFiring(now.plusSeconds(30))))
        assertEquals(store.states(), restarted.states())
        assertEquals(1L, restarted.alertCounts().undelivered)
    }

    @Test
    fun `같은 고유 키의 알림은 두 번 기록해도 한 건이다`() {
        val same = alert("resource:$server:memory@1")
        store.judgeRound(now) { JudgeResult(listOf(state()), listOf(same)) }
        store.judgeRound(now) { JudgeResult(listOf(state()), listOf(same)) }
        assertEquals(1, store.pendingAlerts(10).size)
    }

    @Test
    fun `상태를 쓴 뒤 알림을 쓰기 전에 실패하면 상태도 되돌려진다`() {
        val failing = ThresholdStore(JdbcTemplate(dataSource)) { point -> if (point == "states_written") throw IllegalStateException("주입한 실패") }
        assertThrows(IllegalStateException::class.java) { failing.judgeRound(now, firstFiring()) }
        assertEquals(emptyList<RuleState>(), store.states(), "알림 없이 상태만 남았다 — 알림을 영영 못 보낸다")
        assertEquals(emptyList<Any>(), store.pendingAlerts(10))
        // 다음 바퀴(실패 없음)가 같은 판정을 처음부터 다시 한다
        assertEquals(RoundOutcome(true, 1), store.judgeRound(now.plusSeconds(30), firstFiring(now.plusSeconds(30))))
    }

    @Test
    fun `알림을 쓴 뒤 커밋 전에 실패하면 알림도 상태도 남지 않는다`() {
        val failing = ThresholdStore(JdbcTemplate(dataSource)) { point -> if (point == "events_written") throw IllegalStateException("주입한 실패") }
        assertThrows(IllegalStateException::class.java) { failing.judgeRound(now, firstFiring()) }
        assertEquals(emptyList<Any>(), store.pendingAlerts(10), "상태 없이 알림만 남았다 — 다음 바퀴가 같은 알림을 또 만든다")
        assertEquals(emptyList<RuleState>(), store.states())
    }

    @Test
    fun `이음매는 한 바퀴에 두 지점을 그 순서로 지난다`() {
        val points = mutableListOf<String>()
        ThresholdStore(JdbcTemplate(dataSource)) { points += it }.judgeRound(now, firstFiring())
        assertEquals(listOf("states_written", "events_written"), points)
    }

    // ── 한 자리만 판정 ─────────────────────────────────────

    @Test
    fun `한 자리가 판정 중이면 다른 자리는 기다리지 않고 판정하지 않으며 그 뒤에도 같은 알림이 두 번 생기지 않는다`() {
        val other = ThresholdStore(JdbcTemplate(pool(2)))
        val judging = CountDownLatch(1)
        val release = CountDownLatch(1)
        var firstOutcome: RoundOutcome? = null
        val first = thread(name = "observe-test-judge-blue") {
            firstOutcome = store.judgeRound(now) { previous -> judging.countDown(); release.await(10, TimeUnit.SECONDS); firstFiring()(previous) }
        }
        assertTrue(judging.await(10, TimeUnit.SECONDS))
        val started = System.nanoTime()
        val secondOutcome = other.judgeRound(now) { fail("잠금을 얻지 못한 자리가 판정했다") }
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(2), "잠금을 기다렸다")
        assertEquals(RoundOutcome(false, 0), secondOutcome)
        release.countDown()
        first.join(10_000)
        assertEquals(RoundOutcome(true, 1), firstOutcome)
        assertEquals(RoundOutcome(true, 0), other.judgeRound(now.plusSeconds(1), firstFiring(now.plusSeconds(1))))
        assertEquals(1, store.pendingAlerts(10).size)
    }

    @Test
    fun `두 자리가 동시에 여러 번 돌아도 넘음 알림은 하나다`() {
        val stores = listOf(store, ThresholdStore(JdbcTemplate(pool(4))))
        val judgedRounds = AtomicInteger()
        val failures = ConcurrentHashMap.newKeySet<String>()
        val workers = stores.flatMap { candidate ->
            (1..2).map { index ->
                thread(name = "observe-test-judge-$index") {
                    repeat(25) { round ->
                        val at = now.plusMillis(round.toLong())
                        runCatching { candidate.judgeRound(at, firstFiring(at)) }
                            .onSuccess { if (it.judged) judgedRounds.incrementAndGet() }
                            .onFailure { failures += "${it.javaClass.simpleName}: ${it.message}" }
                    }
                }
            }
        }
        workers.forEach { it.join(60_000) }
        assertEquals(emptySet<String>(), failures, "동시 판정이 실패했다")
        assertTrue(judgedRounds.get() >= 1)
        assertEquals(1, store.pendingAlerts(100).size, "같은 넘음의 알림이 두 번 생겼다")
        assertEquals(1, store.states().count { it.rule == ResourceRule.MEMORY })
    }

    // ── 보내기 ────────────────────────────────────────────

    private fun enqueue(vararg eventIds: String) =
        store.judgeRound(now) { JudgeResult(listOf(state()), eventIds.mapIndexed { index, id -> alert(id, createdAt = now.plusMillis(index.toLong())) }) }

    private class ScriptedSender(private val outcomes: Map<String, SendOutcome>) : AlertSender {
        val sent = mutableListOf<String>()
        override fun send(alert: ResourceAlert): SendOutcome { synchronized(sent) { sent += alert.eventId }; return outcomes[alert.eventId] ?: SendOutcome.Delivered }
    }

    @Test
    fun `받은 것은 지우고 못 보낸 것은 횟수와 사유를 남겨 다음 바퀴에 다시, 거절된 것은 세고 다시 보내지 않는다`() {
        enqueue("e-1", "e-2", "e-3")
        val first = ScriptedSender(mapOf("e-2" to SendOutcome.Retry("http 503"), "e-3" to SendOutcome.Rejected("http 422")))
        assertEquals(DispatchOutcome(true, 1, 1, 1), AlertDispatcher(store, first).dispatch(now))
        val left = store.pendingAlerts(10)
        assertEquals(listOf("e-2"), left.map { it.alert.eventId })
        assertEquals(1 to "http 503", left.single().let { it.attempts to it.lastFailure })
        assertEquals(AlertCounts(1, 1), store.alertCounts())

        val second = ScriptedSender(emptyMap())
        assertEquals(DispatchOutcome(true, 1, 0, 0), AlertDispatcher(store, second).dispatch(now.plusSeconds(30)))
        assertEquals(listOf("e-2"), second.sent, "받은 것이나 거절된 것을 다시 보냈다")
        assertEquals(AlertCounts(0, 1), store.alertCounts())
        assertEquals(DispatchOutcome(true, 0, 0, 0), AlertDispatcher(store, second).dispatch(now.plusSeconds(60)))
    }

    @Test
    fun `두 자리가 동시에 보내도 알림마다 한 번씩만 보낸다`() {
        enqueue(*(1..12).map { "e-$it" }.toTypedArray())
        val sent = ConcurrentHashMap<String, AtomicInteger>()
        val slow = object : AlertSender {
            override fun send(alert: ResourceAlert): SendOutcome {
                sent.computeIfAbsent(alert.eventId) { AtomicInteger() }.incrementAndGet()
                Thread.sleep(40)
                return SendOutcome.Delivered
            }
        }
        val dispatchers = listOf(AlertDispatcher(store, slow, batchSize = 4), AlertDispatcher(ThresholdStore(JdbcTemplate(pool(2))), slow, batchSize = 4))
        val workers = dispatchers.map { dispatcher ->
            thread(name = "observe-test-dispatch") { repeat(20) { dispatcher.dispatch(now); Thread.sleep(5) } }
        }
        workers.forEach { it.join(60_000) }
        assertEquals((1..12).map { "e-$it" }.toSet(), sent.keys)
        assertEquals(emptyMap<String, Int>(), sent.filterValues { it.get() != 1 }.mapValues { it.value.get() }, "같은 알림을 두 번 보냈다")
        assertEquals(AlertCounts(0, 0), store.alertCounts())
    }

    // ── 실제 표본으로 도는 한 바퀴 ─────────────────────────

    private fun sample(at: Instant, memoryPct: Double, server: String = this.server, receivedAt: Instant = at) = ResourceSample(
        StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), server, at, receivedAt, 30_000,
        HostValues(cpuPct = 5.0, load1 = 0.1, memTotal = 16_000_000_000, memUsed = (memoryPct * 160_000_000).roundToLong(), diskTotal = 1_000, diskUsed = 100),
        listOf(ContainerValues("front-app-green", "0123456789ab", "study-note-deploy-system-front", 1.0, 1, 0, "running")),
        listOf(GpuValues(0, 1.0, 1, 2, 30.0)), emptyList(),
    )

    private fun memoryFor(minutes: Long, pct: Double, endingAt: Instant = now) =
        (0 until minutes * 2).map { sample(endingAt.minusSeconds(30 * it), pct) }

    private fun round(sender: AlertSender, states: ThresholdStore = store, expected: Set<String> = emptySet()) =
        ThresholdRound(metricStore, states, ThresholdJudge(), AlertDispatcher(states, sender), states, expected)

    @Test
    fun `지표 저장소가 판정 창의 표본과 서버별 마지막 수신 시각을 준다`() {
        val inside = sample(now.minusSeconds(30), 95.0)
        val edge = sample(now.minus(Duration.ofMinutes(10)), 95.0)
        val longAgo = sample(now.minus(Duration.ofDays(3)), 10.0, server = "ci-agent-9")
        metricStore.insertAll(listOf(inside, edge, longAgo, sample(now, 96.0)))
        val window = metricStore.samplesMeasuredBetween(now.minus(Duration.ofMinutes(10)), now)
        assertEquals(listOf(now.minusSeconds(30), now), window.map { it.measuredAt }, "(from, to] 이 아니거나 측정 시각순이 아니다")
        assertEquals(inside, window.first(), "컨테이너 · GPU 까지 그대로 와야 한다")
        assertEquals(mapOf(server to now, "ci-agent-9" to now.minus(Duration.ofDays(3))), metricStore.lastReceivedOfEveryServer(),
            "하루 넘게 끊긴 서버도 들어야 한다")
    }

    @Test
    fun `메모리가 5분 넘게 높으면 한 바퀴가 알림을 만들어 보내고 다음 바퀴는 다시 보내지 않으며 재시작 뒤에도 그렇다`() {
        metricStore.insertAll(memoryFor(5, 95.0))
        val sender = ScriptedSender(emptyMap())
        val result = round(sender).runOnce(now)
        assertTrue(result.judged)
        assertEquals(1, result.eventsCreated)
        assertEquals(DispatchOutcome(true, 1, 0, 0), result.dispatch)
        assertEquals(listOf("resource:$server:memory"), store.firing().map { "resource:${it.server}:${it.rule}" })
        assertEquals(95.0, store.firing().single().lastValue!!, 0.01)

        metricStore.insertAll(listOf(sample(now.plusSeconds(30), 95.0)))
        round(sender).runOnce(now.plusSeconds(30))
        val restarted = ThresholdStore(JdbcTemplate(pool(2)))
        round(sender, restarted).runOnce(now.plusSeconds(31))
        assertEquals(1, sender.sent.size, "같은 넘음을 다시 보냈다: ${sender.sent}")
    }

    @Test
    fun `알림 서버가 죽어 있으면 알림이 남아 헬스용 건수에 보이고 살아나면 다음 바퀴에 같은 알림이 간다`() {
        metricStore.insertAll(memoryFor(5, 95.0))
        val down = ScriptedSender(emptyMap()).let { recording ->
            object : AlertSender { override fun send(alert: ResourceAlert): SendOutcome { recording.send(alert); return SendOutcome.Retry("ConnectException") } }
        }
        val firstRound = round(down)
        assertEquals(DispatchOutcome(true, 0, 1, 0), firstRound.runOnce(now).dispatch)
        assertEquals(AlertCounts(1, 0), firstRound.alertCounts())
        val pendingId = store.pendingAlerts(10).single().alert.eventId

        val up = ScriptedSender(emptyMap())
        round(up).runOnce(now.plusSeconds(30))
        assertEquals(listOf(pendingId), up.sent, "남은 알림을 다시 보내지 않았거나 새 알림을 더 만들었다")
        assertEquals(AlertCounts(0, 0), store.alertCounts())
    }

    @Test
    fun `밀린 항목으로 지난 한 시간의 넘음이 들어와도 지금이 정상이면 알림을 만들지 않는다`() {
        metricStore.insertAll((20 until 120).map { sample(now.minusSeconds(30L * it), 99.0, receivedAt = now.minusSeconds(5)) } + memoryFor(10, 30.0))
        val sender = ScriptedSender(emptyMap())
        val result = round(sender).runOnce(now)
        assertEquals(0, result.eventsCreated)
        assertEquals(emptyList<String>(), sender.sent)
        assertEquals(emptyList<FiringRule>(), store.firing())
    }

    @Test
    fun `한 번도 보내지 않은 기대 서버는 끊김으로 알린다`() {
        val sender = ScriptedSender(emptyMap())
        round(sender, expected = setOf("ci-agent-164")).runOnce(now)
        assertEquals(listOf(FiringRule("ci-agent-164", "silent", null, "warning", now, null)).map { it.server to it.rule },
            store.firing().map { it.server to it.rule })
        assertEquals(1, sender.sent.size)
    }

    @Test
    fun `latest 의 firing 에 저장된 넘음이 나온다`() {
        metricStore.insertAll(memoryFor(5, 95.0))
        round(ScriptedSender(emptyMap())).runOnce(now)
        val response = MetricController(metricStore, "secret", "", store).latest(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }, now)
        @Suppress("UNCHECKED_CAST")
        val firing = ((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["firing"] as List<Map<String, Any?>>
        assertEquals(1, firing.size, "$firing")
        assertEquals(mapOf("server" to server, "rule" to "memory", "target" to null, "severity" to "warning", "since" to now.toString()),
            firing.single() - "last_value")
        assertEquals(95.0, (firing.single()["last_value"] as Number).toDouble(), 0.01)
    }

    @Test
    fun `DB 에 닿지 못해도 한 바퀴는 예외 없이 실패 종류를 남긴다`() {
        val closed = pool(1).also { it.close() }
        val unreachable = ThresholdStore(JdbcTemplate(closed))
        val result = ThresholdRound(JdbcTemplate(closed).let { MetricStore(it, it, it) }, unreachable, ThresholdJudge(),
            AlertDispatcher(unreachable, ScriptedSender(emptyMap())), unreachable, emptySet()).runOnce(now)
        assertFalse(result.judged)
        assertNotNull(result.judgeFailure)
        assertNotNull(result.dispatchFailure)
    }
}
