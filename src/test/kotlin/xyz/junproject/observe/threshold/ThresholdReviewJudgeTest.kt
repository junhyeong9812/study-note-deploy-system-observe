package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant

/**
 * 명세 §15 · §15.1 (리뷰 루프 1) 의 판정 규칙 — 재시작 해소는 컨테이너 갈래가 온전한 표본(절반 이상 · 가장 최근도)에서만,
 * 끊김 판정을 하지 않는 동안(시작 직후)은 끊김을 보류하고 다른 규칙은 판정, 디스크가 올라갈 때는 위험 하나만.
 */
class ThresholdReviewJudgeTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val server = "ci-agent-158"
    private val judge = ThresholdJudge()
    private var sequence = 0

    private fun container(id: String, restarts: Long, name: String = "front-app-green") =
        ContainerValues(name, id, "study-note-deploy-system-front", 1.0, 1, restarts, "running")

    private fun sample(at: Instant, containers: List<ContainerValues>, missing: List<String> = emptyList(), memoryPct: Double = 10.0) = ResourceSample(
        StreamId(at.toEpochMilli(), sequence++), server, at, at, 30_000,
        HostValues(cpuPct = 10.0, load1 = 0.1, memTotal = 1_000_000, memUsed = (memoryPct * 10_000).toLong(), diskTotal = 1_000, diskUsed = 100),
        containers, emptyList(), missing,
    )

    /** 10분 창의 표본 스물(번호 0 이 가장 오래된 것) */
    private fun window(make: (Int, Instant) -> ResourceSample) = (19 downTo 0).mapIndexed { index, ago -> make(index, now.minusSeconds(30L * ago)) }

    private val restartsFiring = RuleState(server, ResourceRule.RESTARTS, "aaaaaaaaaaaa", "front-app-green", true,
        now.minus(Duration.ofMinutes(20)), now.minus(Duration.ofMinutes(20)), 4.0)

    private fun judged(samples: List<ResourceSample>, previous: List<RuleState>, silentJudging: Boolean = true, expected: Set<String> = emptySet(),
                       lastReceived: Map<String, Instant> = samples.groupBy { it.server }.mapValues { (_, list) -> list.maxOf { it.receivedAt } }) =
        judge.judge(JudgeInput(now, samples, lastReceived, expected, silentJudging), previous)

    private val other = listOf(container("bbbbbbbbbbbb", 0, name = "backend-app-blue"))

    // ── 재시작 규칙의 해소 ─────────────────────────────

    @Test
    fun `창의 표본이 전부 컨테이너를 읽지 못했으면 울리는 ID 가 없어도 보류한다`() {
        val result = judged(window { _, at -> sample(at, emptyList(), missing = listOf("containers")) }, listOf(restartsFiring))
        assertEquals(emptyList<ResourceAlert>(), result.events)
        assertTrue(restartsFiring in result.states, "컨테이너를 못 읽은 것을 해소로 봤다")
    }

    @Test
    fun `컨테이너 목록이 잘린 표본(containers_truncated)도 온전하지 않다 — 보류`() {
        val result = judged(window { _, at -> sample(at, other, missing = listOf("containers_truncated")) }, listOf(restartsFiring))
        assertEquals(emptyList<ResourceAlert>(), result.events)
        assertTrue(restartsFiring in result.states)
    }

    @Test
    fun `앞 절반은 온전해도 가장 최근 표본들이 결측이면 지금을 알 수 없어 보류한다`() {
        val samples = window { index, at -> if (index < 10) sample(at, other) else sample(at, emptyList(), missing = listOf("containers")) }
        val result = judged(samples, listOf(restartsFiring))
        assertEquals(emptyList<ResourceAlert>(), result.events)
        assertTrue(restartsFiring in result.states)
    }

    @Test
    fun `온전한 표본이 절반 이상이고 가장 최근도 온전한데 그 ID 가 없으면 해소한다`() {
        val samples = window { index, at -> if (index < 10) sample(at, emptyList(), missing = listOf("containers")) else sample(at, other) }
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), judged(samples, listOf(restartsFiring)).events.map { it.key })
    }

    // ── 끊김 규칙의 시작 ───────────────────────────────

    private val silentFiring = RuleState("ci-agent-164", ResourceRule.SILENT, "", null, true,
        now.minus(Duration.ofHours(3)), now.minus(Duration.ofHours(2)), null)

    @Test
    fun `끊김을 판정하지 않는 동안 한 번도 안 보낸 기대 서버도 울리지 않고 다른 규칙은 판정한다`() {
        val highMemory = window { _, at -> sample(at, other, memoryPct = 95.0) }
        val result = judged(highMemory, emptyList(), silentJudging = false, expected = setOf("ci-agent-164"))
        assertEquals(listOf("resource:$server:memory"), result.events.map { it.key }, "끊김을 울렸거나 다른 규칙까지 멈췄다")
        assertFalse(result.states.any { it.rule == ResourceRule.SILENT && it.firing })
    }

    @Test
    fun `끊김을 판정하지 않는 동안 울리던 끊김은 해소하지도 다시 알리지도 않는다`() {
        val back = judged(emptyList(), listOf(silentFiring), silentJudging = false, expected = setOf("ci-agent-164"),
            lastReceived = mapOf("ci-agent-164" to now.minusSeconds(5)))
        assertEquals(emptyList<ResourceAlert>(), back.events, "보류 중에 해소했다")
        val stillSilent = judged(emptyList(), listOf(silentFiring), silentJudging = false, expected = setOf("ci-agent-164"))
        assertEquals(emptyList<ResourceAlert>(), stillSilent.events, "보류 중에 다시 알렸다(마지막 알림 2시간 전)")
        assertTrue(silentFiring in stillSilent.states)
    }

    @Test
    fun `끊김을 판정하면(기본) 같은 입력에서 울린다 — 보류는 그 칸으로만 정해진다`() {
        assertEquals(listOf("resource:ci-agent-164:silent"), judged(emptyList(), emptyList(), expected = setOf("ci-agent-164")).events.map { it.key })
    }

    // ── 디스크가 올라갈 때 ─────────────────────────────

    @Test
    fun `경고에서 위험으로 오를 때는 위험 하나만 — 경고 해소를 함께 보내지 않는다`() {
        val warning = RuleState(server, ResourceRule.DISK_WARNING, "", null, true, now.minus(Duration.ofMinutes(10)), now.minus(Duration.ofMinutes(10)), 90.0)
        val samples = listOf(now.minusSeconds(30), now).map { at -> sample(at, other).let { it.copy(host = it.host!!.copy(diskUsed = 960)) } }
        val keys = judged(samples, listOf(warning)).events.map { it.key }
        assertEquals(listOf("resource:$server:disk_critical"), keys)
        assertFalse(keys.any { it.endsWith("disk_warning:resolved") })
    }
}

/** 명세 §15.1 — 끊김 판정은 뜬 지 3분이 지나고 한 번이라도 따라잡았을 때부터, 따라잡지 못해도 15분이 지나면 */
class SilentGateTest {
    private val started = Instant.parse("2026-11-24T03:00:00Z")
    private var caughtUp: Instant? = null
    private val gate = SilentGate(started, { caughtUp })

    @Test
    fun `뜬 지 3분 전에는 따라잡았어도 판정하지 않는다`() {
        caughtUp = started.plusSeconds(5)
        assertFalse(gate.active(started.plus(Duration.ofMinutes(3)).minusMillis(1)))
        assertTrue(gate.active(started.plus(Duration.ofMinutes(3))))
    }

    @Test
    fun `3분이 지났어도 한 번도 따라잡지 못했으면 판정하지 않고 15분이 지나면 판정한다`() {
        assertFalse(gate.active(started.plus(Duration.ofMinutes(5))))
        assertFalse(gate.active(started.plus(Duration.ofMinutes(15)).minusMillis(1)))
        assertTrue(gate.active(started.plus(Duration.ofMinutes(15))))
    }

    @Test
    fun `따라잡은 뒤에는 판정한다`() {
        assertFalse(gate.active(started.plus(Duration.ofMinutes(4))))
        caughtUp = started.plus(Duration.ofMinutes(4))
        assertTrue(gate.active(started.plus(Duration.ofMinutes(4)).plusSeconds(1)))
    }
}
