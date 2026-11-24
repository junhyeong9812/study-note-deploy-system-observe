package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.FakeMetricSink
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricDeletion
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricMaintenance
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.metric.RollupOutcome
import java.time.Duration
import java.time.Instant

/**
 * 명세 §16.2 (리뷰 루프 3) — 재시작 해소 · 증가량은 컨테이너 갈래가 온전한 표본만으로(해소의 까닭까지 본다).
 * 루프 2 의 `RestartResolveIdSetTest` 는 "횟수가 늘지 않음"의 다른 길로 통과해 이 결함을 가렸다 — 여기 사례는 그 길로는 해소되지 않게 짰다.
 */
class RestartIntactSamplesTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val server = "ci-agent-158"
    private var sequence = 0

    private fun container(id: String, name: String, restarts: Long) = ContainerValues(name, id, "study-note-deploy-system-front", 1.0, 1, restarts, "running")

    /** 10분 창 스무 표본(번호 0 이 가장 오래됨) — [truncated] 개의 앞 표본은 `containers_truncated` */
    private fun window(truncated: Int, containersAt: (Int) -> List<ContainerValues>) = (19 downTo 0).mapIndexed { index, ago ->
        val at = now.minusSeconds(30L * ago)
        ResourceSample(StreamId(at.toEpochMilli(), sequence++), server, at, at, 30_000,
            HostValues(cpuPct = 1.0, load1 = 0.1, memTotal = 100, memUsed = 10, diskTotal = 100, diskUsed = 10),
            containersAt(index), emptyList(), if (index < truncated) listOf("containers_truncated") else emptyList())
    }

    private val firing = RuleState(server, ResourceRule.RESTARTS, "aaaaaaaaaaaa", "front-app-green", true,
        now.minus(Duration.ofMinutes(20)), now.minus(Duration.ofMinutes(20)), 4.0)

    private fun judged(samples: List<ResourceSample>, previous: List<RuleState>) =
        ThresholdJudge().judge(JudgeInput(now, samples, mapOf(server to now), emptySet()), previous)

    /** 해소 사건의 글(title + detail) — 해소의 까닭은 이 글로 가른다(해소된 상태 줄은 남지 않는다 — 배포마다 쌓이지 않게) */
    private fun resolvedText(result: JudgeResult) = result.events.single { it.key.endsWith(":resolved") }.let { "${it.title} ${it.detail}" }

    /** 컨테이너가 바뀌어 해소한 까닭을 말하는 낱말 — 기존 테스트에 글의 단언이 없어 리뷰어가 본 구현의 title 낱말을 고정한다 */
    private val replacedWord = "바뀜"

    private val replacement = listOf(container("bbbbbbbbbbbb", "front-app-blue", 0))

    @Test
    fun `잘린 표본 8개에 울리던 ID 가 있고 온전한 12개에는 없으면 컨테이너가 바뀐 것으로 해소한다`() {
        val samples = window(8) { index -> if (index < 8) listOf(container("aaaaaaaaaaaa", "front-app-green", 4)) else replacement }
        val result = judged(samples, listOf(firing))
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), result.events.map { it.key }, "잘린 표본의 ID 를 '아직 있음'으로 셌다")
        assertTrue(resolvedText(result).contains(replacedWord), "해소의 까닭이 '컨테이너가 바뀜'이 아니다: ${resolvedText(result)}")
    }

    @Test
    fun `잘린 표본 안에서 그 ID 의 횟수가 늘어도 온전한 표본에 그 ID 가 없으면 컨테이너가 바뀐 것으로 해소한다`() {
        val counts = listOf(4L, 4, 5, 5, 6, 6, 7, 7, 7, 7)
        val samples = window(10) { index -> if (index < 10) listOf(container("aaaaaaaaaaaa", "front-app-green", counts[index])) else replacement }
        val result = judged(samples, listOf(firing))
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), result.events.map { it.key }, "잘린 표본의 증가량으로 넘음을 이어 갔다")
        assertTrue(resolvedText(result).contains(replacedWord), "해소의 까닭이 '컨테이너가 바뀜'이 아니다: ${resolvedText(result)}")
    }

    @Test
    fun `잘린 표본에만 새 ID 의 큰 증가가 보이고 온전한 표본에는 증가가 없으면 울리지 않는다`() {
        val samples = window(10) { index -> listOf(container("cccccccccccc", "llm-app-green", if (index < 10) index.toLong() else 9L)) }
        val result = judged(samples, emptyList())
        assertEquals(emptyList<String>(), result.events.map { it.key }, "잘린 표본의 증가량(0 → 9)으로 울렸다")
    }

    @Test
    fun `온전한 표본에서 횟수가 늘지 않아 해소하면 그 글은 컨테이너가 바뀐 쪽의 글과 다르다`() {
        val notIncreasing = judged(window(0) { listOf(container("aaaaaaaaaaaa", "front-app-green", 8)) }, listOf(firing))
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), notIncreasing.events.map { it.key })
        assertFalse(resolvedText(notIncreasing).contains(replacedWord), "늘지 않아 해소했는데 '바뀜'의 글이다: ${resolvedText(notIncreasing)}")
        // 온전한 표본만으로 컨테이너가 바뀐 경우(루프 1 의 사례) — 두 까닭의 글이 서로 다르다
        val replaced = judged(window(0) { replacement }, listOf(firing))
        assertTrue(resolvedText(replaced).contains(replacedWord), resolvedText(replaced))
        assertNotEquals(resolvedText(notIncreasing), resolvedText(replaced))
    }
}

/** 명세 §16.2 — 5분 평균은 실패한 구간이 하나도 없는 바퀴만 성공(성공 시각을 올리고 사유를 비운다), 실패가 있으면 구간 수와 첫 실패의 종류를 남긴다 */
class RollupSuccessTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    private class Scripted(var outcome: RollupOutcome) : MetricMaintenance {
        override fun rollUp(now: Instant): Int = outcome.computed
        override fun rollUpBuckets(now: Instant) = outcome
        override fun deleteExpired(now: Instant) = MetricDeletion(0, 0)
    }

    private val unreachableLogStore = org.springframework.jdbc.core.JdbcTemplate(object : org.springframework.jdbc.datasource.AbstractDataSource() {
        override fun getConnection(): java.sql.Connection = throw java.sql.SQLException("Connection refused")
        override fun getConnection(username: String?, password: String?): java.sql.Connection = getConnection()
    }).let { xyz.junproject.observe.log.LogStore(it, it, it) }

    private val maintenance = Scripted(RollupOutcome(3, 0))
    private val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), unreachableLogStore, metrics = maintenance)

    private fun healthFailure(): Any? {
        val stream = FakeMetricStream()
        val collector = MetricCollector(stream, FakeMetricSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        return MetricHealth(collector, { 0L }, { 0L }, { null }, { emptyMap() }, { schedule.lastMetricRollup }, { schedule.lastMetricRollupSuccessAt }, { null },
            startedAt = now).report(now)["last_rollup_failure"]
    }

    @Test
    fun `실패가 없는 바퀴는 성공 시각을 올리고 사유를 비운다 — 계산할 구간이 없어도`() {
        schedule.rollUpMetrics(now)
        assertEquals(now, schedule.lastMetricRollupSuccessAt)
        maintenance.outcome = RollupOutcome(0, 0)
        schedule.rollUpMetrics(now.plusSeconds(60))
        assertEquals(now.plusSeconds(60), schedule.lastMetricRollupSuccessAt)
        assertNull(schedule.lastMetricRollup?.failure)
        assertEquals("", healthFailure())
    }

    @Test
    fun `모든 구간이 실패한 바퀴는 성공 시각을 올리지 않고 실패 구간 수와 첫 실패의 종류를 남긴다`() {
        schedule.rollUpMetrics(now)
        maintenance.outcome = RollupOutcome(0, 2, "DataIntegrityViolationException")
        schedule.rollUpMetrics(now.plusSeconds(60))
        assertEquals(now, schedule.lastMetricRollupSuccessAt, "실패한 바퀴가 성공 시각을 올렸다")
        val failure = healthFailure() as String
        assertTrue(failure.contains("2") && failure.contains("DataIntegrityViolationException"), "실패 사유에 구간 수 · 종류가 없다: '$failure'")
    }

    @Test
    fun `일부만 실패해도 성공이 아니다`() {
        schedule.rollUpMetrics(now)
        maintenance.outcome = RollupOutcome(3, 1, "DataIntegrityViolationException")
        schedule.rollUpMetrics(now.plusSeconds(60))
        assertEquals(now, schedule.lastMetricRollupSuccessAt, "일부 실패한 바퀴가 성공 시각을 올렸다")
        assertTrue((healthFailure() as String).contains("1"))
        maintenance.outcome = RollupOutcome(3, 0)
        schedule.rollUpMetrics(now.plusSeconds(120))
        assertEquals(now.plusSeconds(120), schedule.lastMetricRollupSuccessAt)
        assertEquals("", healthFailure(), "실패 없는 바퀴 뒤에도 사유가 남았다")
    }
}

/** 명세 §16.2 — 한 바퀴는 차례대로 대상마다 머리 한 건씩, 다 돌면 처음 대상으로 돌아가 다음 건(같은 대상 안의 순서는 그대로, 다시 시도가 난 대상은 그 바퀴에서 빠짐) */
class AlertDispatchRoundRobinTest {
    private val start = Instant.parse("2026-11-24T03:00:00Z")
    private var clock = start
    private var sequence = 0

    private fun alert(eventId: String, server: String) =
        ResourceAlert(eventId, "resource:$server:memory", "resource_memory", AlertSeverity.WARNING, "t", "d", server, ResourceRule.MEMORY, null,
            start.plusMillis((sequence++).toLong()))

    private class Queue(alerts: List<ResourceAlert>) : AlertQueue {
        val queued = alerts.mapIndexed { index, alert -> QueuedAlert(index + 1L, alert, 0, null) }.toMutableList()
        val lastAttempt = mutableMapOf<AlertTarget, Instant>()
        private fun attempted(id: Long, at: Instant) { lastAttempt[targetOf(queued.first { it.id == id }.alert)] = at }
        override fun pendingAlerts(limit: Int) = queued.take(limit)
        override fun pendingByTarget(perTarget: Int) = targetQueues(queued, lastAttempt, perTarget)
        override fun markDelivered(id: Long, at: Instant) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun markRetry(id: Long, at: Instant, reason: String) { attempted(id, at) }
        override fun markRejected(id: Long, at: Instant, reason: String) { attempted(id, at); queued.removeIf { it.id == id } }
        override fun <T> whileHoldingDispatchLock(block: () -> T): T? = block()
        override fun alertCounts() = AlertCounts(queued.size.toLong(), 0)
    }

    private fun run(alerts: List<ResourceAlert>, outcome: (ResourceAlert) -> SendOutcome = { SendOutcome.Delivered }): List<String> {
        val sent = mutableListOf<String>()
        val sender = object : AlertSender {
            override fun send(alert: ResourceAlert): SendOutcome { sent += alert.eventId; clock = clock.plusSeconds(3); return outcome(alert) }
        }
        AlertDispatcher(Queue(alerts), sender, clock = { clock }).dispatch(start)
        return sent
    }

    @Test
    fun `알림이 많이 밀린 대상 뒤에서 다른 대상이 기다리지 않는다 — A1 B1 A2 A3`() {
        val alerts = (1..12).map { alert("A$it", "server-a") } + alert("B1", "server-b")          // B 의 머리는 A 보다 늦다
        val sent = run(alerts)
        assertEquals(listOf("A1", "B1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"), sent)
    }

    @Test
    fun `대상 셋에 넷씩이면 대상을 돌아가며 한 건씩이고 같은 대상 안의 순서는 그대로`() {
        val alerts = listOf("A", "B", "C").flatMap { target -> (1..4).map { alert("$target$it", "server-$target") } }
        assertEquals(listOf("A1", "B1", "C1", "A2", "B2", "C2", "A3", "B3", "C3", "A4"), run(alerts))
    }

    @Test
    fun `다시 시도가 난 대상은 그 바퀴에서 빠지고 나머지는 계속 돈다`() {
        val alerts = listOf("A", "B", "C").flatMap { target -> (1..3).map { alert("$target$it", "server-$target") } }
        assertEquals(listOf("A1", "B1", "C1", "B2", "C2", "B3", "C3"), run(alerts) { if (it.eventId == "A1") SendOutcome.Retry("http 503") else SendOutcome.Delivered })
    }
}
