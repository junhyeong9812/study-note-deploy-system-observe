package xyz.junproject.observe.threshold

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant
import kotlin.math.roundToLong

/**
 * 명세 §2.4 · §11.4 — 다섯 규칙의 넘음 · 해소 · 다시 알림, 디스크 세 단계와 되돌림 폭, 지속 시간은 측정 시각으로, 결측은 미룸,
 * 지금의 상태만(지난 구간을 되짚지 않음), 재시작은 컨테이너 ID 별, 끊김은 수신 시각 · 기대 서버. 판정은 순수 함수 — 시각은 입력으로 준다.
 */
class ThresholdJudgeTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")
    private val server = "ci-agent-158"
    private val judge = ThresholdJudge()
    private var sequence = 0

    private fun sample(
        at: Instant, memoryPct: Double? = 10.0, cpuPct: Double? = 10.0, diskPct: Double? = 10.0,
        containers: List<ContainerValues> = emptyList(), server: String = this.server,
    ) = ResourceSample(
        id = StreamId(at.toEpochMilli(), sequence++), server = server, measuredAt = at, receivedAt = at, intervalMs = 30_000,
        host = HostValues(
            cpuPct = cpuPct, load1 = 0.1,
            memTotal = memoryPct?.let { TOTAL }, memUsed = memoryPct?.let { (it * TOTAL / 100).roundToLong() },
            diskTotal = diskPct?.let { TOTAL }, diskUsed = diskPct?.let { (it * TOTAL / 100).roundToLong() },
        ),
        containers = containers, gpus = emptyList(), missing = emptyList(),
    )

    /** 30초마다 [count] 개, 가장 최근이 now − [endingAgo] — 오래된 것부터 [make] 를 부른다(번호 0 이 가장 오래된 표본) */
    private fun every30Seconds(count: Int, endingAgo: Duration = Duration.ZERO, make: (Instant) -> ResourceSample) =
        (count - 1 downTo 0).map { make(now.minus(endingAgo).minusSeconds(30L * it)) }

    /** 표본의 측정 · 수신 시각을 [by] 만큼 옮긴다 — 시계가 흐른 뒤의 같은 모양 */
    private fun shifted(samples: List<ResourceSample>, by: Duration) =
        samples.map { it.copy(measuredAt = it.measuredAt.plus(by), receivedAt = it.receivedAt.plus(by)) }

    private fun input(samples: List<ResourceSample>, at: Instant = now, expected: Set<String> = emptySet(),
                      lastReceived: Map<String, Instant> = samples.groupBy { it.server }.mapValues { (_, list) -> list.maxOf { it.receivedAt } }) =
        JudgeInput(at, samples, lastReceived, expected)

    private fun judged(samples: List<ResourceSample>, previous: List<RuleState> = emptyList(), at: Instant = now, expected: Set<String> = emptySet()) =
        judge.judge(input(samples, at, expected), previous)

    private fun JudgeResult.keys() = events.map { it.key }
    private fun JudgeResult.firing() = states.filter { it.firing }.map { it.rule to it.server }.toSet()

    private fun firingState(rule: ResourceRule, since: Instant = now.minus(Duration.ofMinutes(20)), notified: Instant = since,
                            target: String = "", targetName: String? = null, server: String = this.server) =
        RuleState(server, rule, target, targetName, true, since, notified, 99.0)

    // ── 디스크(세 단계 · 연속 2회 · 되돌림 폭) ───────────

    private fun disk(vararg percents: Double?) = percents.mapIndexed { index, pct -> sample(now.minusSeconds(30L * (percents.size - 1 - index)), diskPct = pct) }

    @Test
    fun `디스크가 정확히 85 이상으로 연속 2회면 경고 알림 하나`() {
        val result = judged(disk(85.0, 85.0))
        assertEquals(listOf("resource:$server:disk_warning"), result.keys())
        val alert = result.events.single()
        assertEquals("resource_disk", alert.kind)
        assertEquals(AlertSeverity.WARNING, alert.severity)
        assertEquals(ResourceRule.DISK_WARNING, alert.rule)
        assertEquals(setOf(ResourceRule.DISK_WARNING to server), result.firing())
    }

    @Test
    fun `디스크가 한 번만 넘거나 바로 아래면 울리지 않는다`() {
        assertEquals(emptyList<String>(), judged(disk(84.99, 85.0)).keys())
        assertEquals(emptyList<String>(), judged(disk(85.0, 84.99)).keys())
        assertEquals(emptyList<String>(), judged(disk(84.99, 84.99)).keys())
        assertEquals(emptySet<Any>(), judged(disk(85.0, 84.99)).firing())
    }

    @Test
    fun `디스크 표본이 하나뿐이거나 2분보다 오래됐으면 판정을 미루고 상태를 그대로 둔다`() {
        val warning = firingState(ResourceRule.DISK_WARNING)
        assertEquals(emptyList<String>(), judged(disk(99.0)).keys(), "한 번 관측으로 울렸다")
        val stale = listOf(sample(now.minusSeconds(150), diskPct = 10.0), sample(now.minusSeconds(121), diskPct = 10.0))
        val result = judged(stale, listOf(warning))
        assertEquals(emptyList<String>(), result.keys(), "오래된 표본으로 해소했다")
        assertTrue(warning in result.states)
    }

    @Test
    fun `가장 최근 값이 null 이면 그 앞의 두 값으로 본다`() {
        assertEquals(listOf("resource:$server:disk_warning"), judged(disk(90.0, 90.0, null)).keys())
    }

    @Test
    fun `정상에서 곧바로 95 이상 연속 2회면 위험 알림 하나만 — 경고를 따로 보내지 않는다`() {
        val result = judged(disk(95.0, 96.0))
        assertEquals(listOf("resource:$server:disk_critical"), result.keys())
        assertEquals(AlertSeverity.CRITICAL, result.events.single().severity)
        assertEquals(setOf(ResourceRule.DISK_CRITICAL to server), result.firing())
    }

    @Test
    fun `경고에서 95 이상 연속 2회면 위험으로 오르며 알림 하나`() {
        val result = judged(disk(95.0, 95.0), listOf(firingState(ResourceRule.DISK_WARNING)))
        assertEquals(listOf("resource:$server:disk_critical"), result.keys())
        assertEquals(setOf(ResourceRule.DISK_CRITICAL to server), result.firing())
    }

    @Test
    fun `경고는 82 아래로 연속 2회여야 해소되고 그 사이(82~85)는 경고가 이어진다`() {
        val warning = listOf(firingState(ResourceRule.DISK_WARNING))
        assertEquals(emptyList<String>(), judged(disk(84.0, 83.0), warning).keys())
        assertEquals(setOf(ResourceRule.DISK_WARNING to server), judged(disk(84.0, 83.0), warning).firing())
        assertEquals(emptyList<String>(), judged(disk(82.0, 82.0), warning).keys(), "82 는 '아래'가 아니다")
        assertEquals(emptyList<String>(), judged(disk(81.99, 82.0), warning).keys())
        val resolved = judged(disk(81.99, 81.0), warning)
        assertEquals(listOf("resource:$server:disk_warning:resolved"), resolved.keys())
        assertEquals(AlertSeverity.INFO, resolved.events.single().severity)
        assertEquals(emptySet<Any>(), resolved.firing())
    }

    @Test
    fun `위험은 92 아래로 연속 2회면 위험 해소 · 경고 유지 알림 하나로 내려온다`() {
        val critical = listOf(firingState(ResourceRule.DISK_CRITICAL))
        assertEquals(emptyList<String>(), judged(disk(93.0, 92.0), critical).keys(), "92 는 '아래'가 아니다")
        val down = judged(disk(91.99, 90.0), critical)
        assertEquals(listOf("resource:$server:disk_critical:resolved"), down.keys())
        assertEquals(AlertSeverity.WARNING, down.events.single().severity, "경고는 이어진다")
        assertEquals(setOf(ResourceRule.DISK_WARNING to server), down.firing())
    }

    @Test
    fun `위험에서 82 아래로 곧바로 내려오면 알림은 하나이고 정상이다`() {
        val result = judged(disk(50.0, 50.0), listOf(firingState(ResourceRule.DISK_CRITICAL)))
        assertEquals(listOf("resource:$server:disk_critical:resolved"), result.keys())
        assertEquals(AlertSeverity.INFO, result.events.single().severity)
        assertEquals(emptySet<Any>(), result.firing())
    }

    // ── 메모리(90% 가 5분) ─────────────────────────────

    private fun memory(count: Int, endingAgo: Duration = Duration.ZERO, value: (Int) -> Double?): List<ResourceSample> {
        var index = 0
        return every30Seconds(count, endingAgo) { at -> sample(at, memoryPct = value(index++)) }
    }

    @Test
    fun `메모리가 5분 동안 90 이상이면 경고 알림 하나이고 마지막 값을 남긴다`() {
        val samples = memory(10) { if (it == 9) 93.5 else 90.0 }               // 정확히 90 도 '이상'
        val result = judged(samples)
        assertEquals(listOf("resource:$server:memory"), result.keys())
        val alert = result.events.single()
        assertEquals("resource_memory", alert.kind)
        assertEquals(AlertSeverity.WARNING, alert.severity)
        val state = result.states.single { it.rule == ResourceRule.MEMORY }
        assertTrue(state.firing)
        assertEquals(now, state.since)
        assertEquals(now, state.lastNotifiedAt)
        assertEquals(93.5, state.lastValue!!, 0.01)
    }

    @Test
    fun `중간에 한 번이라도 90 아래면 이어진 것이 아니다 — 다시 센다`() {
        assertEquals(emptyList<String>(), judged(memory(10) { if (it == 4) 89.99 else 95.0 }).keys())
        // 끊긴 표본이 창 밖으로 나가야(그 뒤로 다시 5분) 울린다
        val restarted = memory(10) { 95.0 } + listOf(sample(now.minus(Duration.ofMinutes(5)), memoryPct = 50.0))
        assertEquals(listOf("resource:$server:memory"), judged(restarted).keys(), "창 밖(정확히 5분 전)의 낮은 값은 보지 않는다")
    }

    @Test
    fun `막 들어오기 시작해 창을 덮지 못하면(2분 30초뿐) 울리지 않는다`() {
        assertEquals(emptyList<String>(), judged(memory(5) { 99.0 }).keys())
    }

    @Test
    fun `표본이 기대 수의 절반이면 판정하고 절반에 못 미치면 미룬다`() {
        // 1분 간격 다섯 개(기대 10 의 절반)로 창을 덮었다
        val half = (0 until 5).map { sample(now.minusSeconds(60L * it + 30), memoryPct = 95.0) }.reversed()
        assertEquals(listOf("resource:$server:memory"), judged(half).keys())
        val tooFew = (0 until 4).map { sample(now.minusSeconds(80L * it + 30), memoryPct = 95.0) }.reversed()
        assertEquals(emptyList<String>(), judged(tooFew).keys())
    }

    @Test
    fun `값이 null 인 표본은 결측이다 — 넘음으로도 해소로도 세지 않는다`() {
        val mostlyNull = memory(10) { if (it % 2 == 0 && it > 0) 95.0 else null }               // 값 있는 것 넷
        assertEquals(emptyList<String>(), judged(mostlyNull).keys())
        val firing = listOf(firingState(ResourceRule.MEMORY))
        val allNull = memory(10) { null }
        val result = judged(allNull, firing)
        assertEquals(emptyList<String>(), result.keys(), "결측을 해소로 봤다")
        assertEquals(firing, result.states.filter { it.rule == ResourceRule.MEMORY })
    }

    @Test
    fun `메모리 해소는 85 아래가 5분 이어질 때 — 다른 키로 한 번`() {
        val firing = listOf(firingState(ResourceRule.MEMORY))
        assertEquals(emptyList<String>(), judged(memory(10) { if (it == 3) 85.0 else 70.0 }, firing).keys(), "85 는 '아래'가 아니다")
        assertEquals(emptyList<String>(), judged(memory(10) { 87.0 }, firing).keys(), "90 아래지만 85 이상이면 넘음이 이어진다")
        assertEquals(setOf(ResourceRule.MEMORY to server), judged(memory(10) { 87.0 }, firing).firing())
        val resolved = judged(memory(10) { 84.9 }, firing)
        assertEquals(listOf("resource:$server:memory:resolved"), resolved.keys())
        assertEquals(AlertSeverity.INFO, resolved.events.single().severity)
        assertEquals(emptySet<Any>(), resolved.firing())
    }

    // ── CPU(90% 가 10분, 해소 80% 아래 5분) ──────────────

    private fun cpu(count: Int, value: (Int) -> Double?): List<ResourceSample> {
        var index = 0
        return every30Seconds(count) { at -> sample(at, cpuPct = value(index++)) }
    }

    @Test
    fun `CPU 는 10분을 덮어야 울리고 5분만으로는 울리지 않는다`() {
        assertEquals(emptyList<String>(), judged(cpu(10) { 99.0 }).keys())
        assertEquals(emptyList<String>(), judged(cpu(19) { 99.0 }).keys(), "9분 30초는 10분이 아니다")
        val result = judged(cpu(20) { 90.0 })
        assertEquals(listOf("resource:$server:cpu"), result.keys())
        assertEquals("resource_cpu", result.events.single().kind)
    }

    @Test
    fun `CPU 해소는 80 아래가 5분 — 80 이 섞이면 이어진다`() {
        val firing = listOf(firingState(ResourceRule.CPU))
        assertEquals(emptyList<String>(), judged(cpu(10) { if (it == 7) 80.0 else 10.0 }, firing).keys())
        assertEquals(listOf("resource:$server:cpu:resolved"), judged(cpu(10) { 79.9 }, firing).keys())
    }

    // ── 컨테이너 재시작(ID 별, 10분 안에 +3) ───────────────

    private fun container(id: String, restarts: Long?, name: String = "front-app-green") =
        ContainerValues(name, id, "study-note-deploy-system-front", 1.0, 1, restarts, "running")

    private fun restarts(count: Int, endingAgo: Duration = Duration.ZERO, value: (Int) -> List<ContainerValues>): List<ResourceSample> {
        var index = 0
        return every30Seconds(count, endingAgo) { at -> sample(at, containers = value(index++)) }
    }

    @Test
    fun `한 컨테이너의 재시작 횟수가 10분 안에 3 늘면 경고 — key 와 target 은 컨테이너 이름`() {
        val samples = restarts(20) { index -> listOf(container("aaaaaaaaaaaa", if (index < 10) 4L else 7L)) }
        val result = judged(samples)
        assertEquals(listOf("resource:$server:restarts:front-app-green"), result.keys())
        assertEquals("resource_restarts", result.events.single().kind)
        assertEquals("front-app-green", result.events.single().targetName)
        val state = result.states.single { it.rule == ResourceRule.RESTARTS }
        assertEquals("aaaaaaaaaaaa", state.target)
        assertEquals("front-app-green", state.targetName)
    }

    @Test
    fun `2 만 늘었거나 창 밖에서 는 것은 울리지 않는다`() {
        assertEquals(emptyList<String>(), judged(restarts(20) { listOf(container("aaaaaaaaaaaa", if (it < 10) 4L else 6L)) }).keys())
        // 10분보다 앞에서 늘고 창 안에서는 그대로
        val before = listOf(sample(now.minus(Duration.ofMinutes(11)), containers = listOf(container("aaaaaaaaaaaa", 0))))
        assertEquals(emptyList<String>(), judged(before + restarts(20) { listOf(container("aaaaaaaaaaaa", 9L)) }).keys())
    }

    @Test
    fun `컨테이너가 새 ID 로 바뀌면 재시작 수를 이어 세지 않는다 — 배포는 재시작이 아니다`() {
        val samples = restarts(20) { index ->
            if (index < 10) listOf(container("aaaaaaaaaaaa", 0)) else listOf(container("bbbbbbbbbbbb", 5, name = "front-app-blue"))
        }
        assertEquals(emptyList<String>(), judged(samples).keys())
    }

    @Test
    fun `새로 뜬 컨테이너의 표본이 절반에 못 미치면 늘었어도 미룬다`() {
        val young = restarts(6) { listOf(container("cccccccccccc", it.toLong())) }        // 3분: 0 → 5
        assertEquals(emptyList<String>(), judged(young).keys())
    }

    @Test
    fun `재시작이 10분 동안 늘지 않으면 해소, 조금이라도 늘면 이어진다`() {
        val firing = listOf(firingState(ResourceRule.RESTARTS, target = "aaaaaaaaaaaa", targetName = "front-app-green"))
        assertEquals(emptyList<String>(), judged(restarts(20) { listOf(container("aaaaaaaaaaaa", if (it < 19) 7L else 8L)) }, firing).keys())
        val resolved = judged(restarts(20) { listOf(container("aaaaaaaaaaaa", 8L)) }, firing)
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), resolved.keys())
        assertEquals(emptySet<Any>(), resolved.firing())
    }

    @Test
    fun `넘어 있던 컨테이너가 사라지고 서버는 계속 보내면 해소한다`() {
        val firing = listOf(firingState(ResourceRule.RESTARTS, target = "aaaaaaaaaaaa", targetName = "front-app-green"))
        val replaced = restarts(20) { listOf(container("bbbbbbbbbbbb", 0, name = "front-app-blue")) }
        assertEquals(listOf("resource:$server:restarts:front-app-green:resolved"), judged(replaced, firing).keys())
    }

    @Test
    fun `restarts 가 null 인 표본은 세지 않는다`() {
        val samples = restarts(20) { index -> listOf(container("aaaaaaaaaaaa", if (index % 3 == 0) null else if (index < 10) 1L else 4L)) }
        assertEquals(listOf("resource:$server:restarts:front-app-green"), judged(samples).keys())
    }

    // ── 지표 끊김(수신 시각 3분) ──────────────────────────

    @Test
    fun `한 번도 보내지 않은 기대 서버는 끊김이다`() {
        val result = judged(emptyList(), expected = setOf("ci-agent-164"))
        assertEquals(listOf("resource:ci-agent-164:silent"), result.keys())
        assertEquals("resource_silent", result.events.single().kind)
        assertEquals(setOf(ResourceRule.SILENT to "ci-agent-164"), result.firing())
    }

    @Test
    fun `마지막 수신이 정확히 3분 전이면 아니고 1ms 넘으면 끊김 — 측정 시각이 아니라 수신 시각`() {
        fun judgedWithReceipt(receivedAgo: Duration) =
            judge.judge(JudgeInput(now, emptyList(), mapOf(server to now.minus(receivedAgo)), emptySet()), emptyList())
        assertEquals(emptyList<String>(), judgedWithReceipt(Duration.ofMinutes(3)).keys())
        assertEquals(listOf("resource:$server:silent"), judgedWithReceipt(Duration.ofMinutes(3).plusMillis(1)).keys())
        // 측정 시각은 최근이어도(밀린 항목) 수신이 오래면 끊김
        val oldReceipt = sample(now.minusSeconds(10)).copy(receivedAt = now.minus(Duration.ofMinutes(4)))
        assertEquals(listOf("resource:$server:silent"),
            judge.judge(JudgeInput(now, listOf(oldReceipt), mapOf(server to oldReceipt.receivedAt), emptySet()), emptyList()).keys())
    }

    @Test
    fun `하루 넘게 끊긴 기대 서버도 판정하고 다시 들어오면 해소한다`() {
        val dayAgo = judge.judge(JudgeInput(now, emptyList(), mapOf(server to now.minus(Duration.ofDays(2))), setOf(server)), emptyList())
        assertEquals(listOf("resource:$server:silent"), dayAgo.keys())
        val back = judge.judge(JudgeInput(now, emptyList(), mapOf(server to now.minusSeconds(5)), setOf(server)), dayAgo.states)
        assertEquals(listOf("resource:$server:silent:resolved"), back.keys())
        assertEquals(AlertSeverity.INFO, back.events.single().severity)
    }

    // ── 지금의 상태만 · 전이 때만 · 다시 알림 ─────────────

    @Test
    fun `같은 상태가 이어지면 다시 보내지 않고 마지막 알림에서 1시간이 되면 같은 key 로 다시 알린다`() {
        val high = memory(10) { 95.0 }
        val first = judged(high)
        assertEquals(1, first.events.size)
        fun at(time: Instant, previous: List<RuleState>) = judge.judge(input(shifted(high, Duration.between(now, time)), time), previous)
        assertEquals(emptyList<String>(), at(now.plusSeconds(30), first.states).keys(), "넘음이 이어지는데 다시 보냈다")
        val quiet = at(now.plus(Duration.ofMinutes(59)).plusSeconds(59), first.states)
        assertEquals(emptyList<String>(), quiet.keys())
        val reminder = at(now.plus(Duration.ofHours(1)), quiet.states)
        assertEquals(listOf("resource:$server:memory"), reminder.keys())
        assertEquals(AlertSeverity.WARNING, reminder.events.single().severity)
        assertEquals(now, reminder.states.single { it.rule == ResourceRule.MEMORY }.since, "다시 알림은 넘음의 시작을 바꾸지 않는다")
        assertEquals(now.plus(Duration.ofHours(1)), reminder.states.single { it.rule == ResourceRule.MEMORY }.lastNotifiedAt)
        assertEquals(emptyList<String>(), at(now.plus(Duration.ofHours(1)).plusSeconds(30), reminder.states).keys())
    }

    @Test
    fun `판정이 세 시간 멈췄다 돌아와도 다시 알림은 한 번뿐이다`() {
        val firing = listOf(firingState(ResourceRule.MEMORY, since = now.minus(Duration.ofHours(4)), notified = now.minus(Duration.ofHours(3))))
        val result = judged(memory(10) { 95.0 }, firing)
        assertEquals(listOf("resource:$server:memory"), result.keys())
    }

    @Test
    fun `밀린 항목으로 지난 한 시간의 넘음이 한꺼번에 들어와도 지금이 정상이면 아무것도 보내지 않는다`() {
        val pastHigh = (20 until 120).map { sample(now.minusSeconds(30L * it), memoryPct = 99.0, cpuPct = 99.0, diskPct = 99.0) }
        val nowNormal = memory(20) { 40.0 }
        val result = judged(pastHigh + nowNormal)
        assertEquals(emptyList<String>(), result.keys())
        assertEquals(emptySet<Any>(), result.firing())
    }

    @Test
    fun `꺼져 있던 동안 넘었다 풀린 것은 보내지 않고 넘어 있던 것이 지금 풀렸으면 해소 하나만`() {
        val firing = listOf(firingState(ResourceRule.MEMORY, since = now.minus(Duration.ofHours(2)), notified = now.minus(Duration.ofHours(2))))
        val result = judged(memory(10) { 40.0 }, firing)
        assertEquals(listOf("resource:$server:memory:resolved"), result.keys())
    }

    @Test
    fun `서버마다 따로 판정하고 앞 상태 중 이번에 보지 않은 것도 그대로 남는다`() {
        val other = firingState(ResourceRule.CPU, server = "ci-agent-9")
        val samples = memory(10) { 95.0 } + every30Seconds(10) { sample(it, memoryPct = 20.0, server = "ci-agent-164") }
        val result = judged(samples, listOf(other))
        assertEquals(listOf("resource:$server:memory"), result.keys())
        assertTrue(other in result.states, "보지 않은 서버의 상태를 잃었다")
    }

    @Test
    fun `같은 입력과 앞 상태면 결과가 같다 — 알림의 고유 키도 같다`() {
        val samples = every30Seconds(10) { sample(it, memoryPct = 95.0, diskPct = 90.0) }
        val first = judged(samples)
        assertEquals(setOf("resource:$server:memory", "resource:$server:disk_warning"), first.keys().toSet())
        val second = judged(samples)
        assertEquals(first, second)
        assertEquals(first.events.size, first.events.map { it.eventId }.toSet().size, "한 바퀴 안에서 고유 키가 겹친다")
        val later = judge.judge(input(samples, now.plusSeconds(1)), emptyList())
        assertNotEquals(first.events.map { it.eventId }.toSet(), later.events.map { it.eventId }.toSet(), "다른 판정의 알림이 같은 고유 키다")
    }

    @Test
    fun `key 는 120자를 넘지 않고 긴 이름이어도 넘음과 해소의 key 가 다르다`() {
        val longServer = "s" + "x".repeat(62)
        val longName = "a" + "b".repeat(127)
        val samples = restarts(20) { index -> listOf(container("aaaaaaaaaaaa", if (index < 10) 0L else 9L, name = longName)) }.map { it.copy(server = longServer) }
        val fired = judged(samples)
        val firingKey = fired.events.single().key
        assertTrue(firingKey.length <= 120 && firingKey.startsWith("resource:"), firingKey)
        val calm = restarts(20) { listOf(container("aaaaaaaaaaaa", 9L, name = longName)) }.map { it.copy(server = longServer) }
        val resolvedKey = judged(calm, fired.states).events.single().key
        assertTrue(resolvedKey.length <= 120 && resolvedKey.endsWith(":resolved"), resolvedKey)
        assertNotEquals(firingKey, resolvedKey)
    }

    // ── 알림 본문 · 수치 ─────────────────────────────────

    @Test
    fun `알림 본문은 알림 서버가 받는 여섯 칸이고 source 는 observe`() {
        val alert = judged(memory(10) { 95.0 }).events.single()
        val body = alert.eventBody()
        assertEquals(setOf("source", "kind", "severity", "key", "title", "detail"), body.keys)
        assertEquals("observe", body["source"])
        assertEquals("resource_memory", body["kind"])
        assertEquals("warning", body["severity"])
        assertEquals("resource:$server:memory", body["key"])
        assertTrue(body["title"]!!.trim().length in 1..200, body["title"])
        assertTrue(body["title"]!!.contains(server), "어느 서버인지 제목에 없다: ${body["title"]}")
        assertTrue(body["detail"]!!.length <= 1500)
        assertTrue(body["kind"]!!.matches(Regex("^[a-z0-9_]{1,40}$")), "알림 서버의 kind 꼴")
    }

    @Test
    fun `수치는 설정으로 바꿀 수 있다`() {
        val lowered = ThresholdJudge(ResourceThresholds(memoryPct = 50.0, memoryResolvePct = 40.0))
        assertEquals(listOf("resource:$server:memory"), lowered.judge(input(memory(10) { 60.0 }), emptyList()).events.map { it.key })
    }

    @Test
    fun `기본 수치는 명세 그대로다`() {
        val defaults = ResourceThresholds()
        assertEquals(listOf(85.0, 95.0, 3.0, 90.0, 85.0, 90.0, 80.0),
            listOf(defaults.diskWarningPct, defaults.diskCriticalPct, defaults.diskResolveMarginPct, defaults.memoryPct, defaults.memoryResolvePct, defaults.cpuPct, defaults.cpuResolvePct))
        assertEquals(2, defaults.diskConsecutive)
        assertEquals(listOf(5L, 5L, 10L, 5L, 10L, 3L, 60L),
            listOf(defaults.memoryFor, defaults.memoryResolveFor, defaults.cpuFor, defaults.cpuResolveFor, defaults.restartWindow, defaults.silentAfter, defaults.remindEvery).map { it.toMinutes() })
        assertEquals(3, defaults.restartIncrease)
        assertEquals(Duration.ofMinutes(10), ThresholdJudge().lookback)
    }

    @Test
    fun `수치가 말이 안 되면 뜰 때 실패한다`() {
        assertThrows(IllegalArgumentException::class.java) { ResourceThresholds(memoryPct = 150.0) }
        assertThrows(IllegalArgumentException::class.java) { ResourceThresholds(memoryResolvePct = 95.0) }    // 해소 기준이 넘음 기준보다 높다
        assertThrows(IllegalArgumentException::class.java) { ResourceThresholds(diskCriticalPct = 80.0) }    // 위험이 경고보다 낮다
        assertThrows(IllegalArgumentException::class.java) { ResourceThresholds(cpuFor = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { ResourceThresholds(restartIncrease = 0) }
    }

    private companion object {
        const val TOTAL = 100_000_000L
    }
}
