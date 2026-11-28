package xyz.junproject.observe.threshold

import xyz.junproject.observe.metric.ResourceSample
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

/**
 * 보낼 표에 드는 알림의 규칙 — 자원 규칙([ResourceRule])과 로그 규칙([LogRule]). [wireName] 은 보낼 표의 `rule` 칸 · 알림 key 의 규칙 자리,
 * [kind] 는 알림 서버 `POST /events` 의 `kind`.
 */
sealed interface AlertRule {
    val wireName: String
    val kind: String

    companion object {
        /** 이 판이 아는 규칙 전부 */
        val entries: List<AlertRule> get() = ResourceRule.entries + LogRule.entries + LogPatternRule.entries

        /** 규칙 글자 → 규칙 — 글자가 정확히 같아야 한다(대소문자 · 공백도). 모르는 글자는 [IllegalStateException] */
        fun fromWireName(wireName: String): AlertRule =
            entries.firstOrNull { it.wireName == wireName } ?: throw IllegalStateException("알 수 없는 규칙: $wireName")
    }
}

/**
 * 임계값 규칙(명세 §2.4 · §11.4). [wireName] 은 알림 key 의 `<규칙>` 자리 · `GET /metrics/latest` 의 `firing[].rule` 글자,
 * [kind] 는 알림 서버 `POST /events` 의 `kind`. 디스크는 단계마다 규칙 하나(경고 · 위험 — 같은 key 를 알림 서버가 10분 억제하므로
 * 단계가 바뀐 알림이 억제되지 않게 key 를 나눈다).
 */

enum class ResourceRule(override val wireName: String, override val kind: String) : AlertRule {
    DISK_WARNING("disk_warning", "resource_disk"),
    DISK_CRITICAL("disk_critical", "resource_disk"),
    MEMORY("memory", "resource_memory"),
    CPU("cpu", "resource_cpu"),
    RESTARTS("restarts", "resource_restarts"),
    SILENT("silent", "resource_silent"),
}

/** 알림 서버의 `severity` 글자(알림 서버가 받는 셋 — info · warning · critical) */
enum class AlertSeverity(val wireName: String) {
    INFO("info"), WARNING("warning"), CRITICAL("critical");

    companion object {
        /** 등급 글자 → 등급 — 글자가 정확히 같아야 한다(대소문자 · 공백도). 모르는 글자는 [IllegalStateException] */
        fun fromWireName(wireName: String): AlertSeverity =
            entries.firstOrNull { it.wireName == wireName } ?: throw IllegalStateException("알 수 없는 등급: $wireName")
    }
}

/**
 * 수치(명세 §2.4 의 표가 기본값). 설정 키는 `observe.metrics.alert.<칸 이름의 케밥꼴>`, 환경 변수는 `METRICS_ALERT_<대문자 밑줄꼴>`
 * (예: `memoryPct` → `observe.metrics.alert.memory-pct` · `METRICS_ALERT_MEMORY_PCT`, 시간은 `5m` · `1h` 꼴).
 *
 * - 디스크: 가장 최근 [diskConsecutive] 개의 값(측정 시각이 `(now − diskRecent, now]` 안, null 제외)이 모두 기준을 넘으면/아래면.
 *   정상 → 경고(모두 ≥ [diskWarningPct]) · → 위험(모두 ≥ [diskCriticalPct], 정상에서 곧바로도) ·
 *   위험 → 경고(모두 < 위험 − [diskResolveMarginPct]) · 경고나 위험 → 정상(모두 < 경고 − [diskResolveMarginPct]).
 * - 메모리 · CPU: 창 `(now − 창, now]` 의 표본(null 제외)이 모두 기준 이상이면 넘음, 해소 창의 표본이 모두 해소 기준 아래면 해소.
 * - 재시작: 컨테이너 ID 하나의 `restarts` 가 창 `(now − restartWindow, now]` 안에서 [restartIncrease] 이상 늘면 넘음, 그 창에서 늘지 않으면 해소.
 * - 끊김: 마지막 수신이 [silentAfter] 보다 오래(엄격히 넘게) 됐거나 한 번도 없으면 넘음.
 * - 넘어 있는 동안 마지막 알림에서 [remindEvery] 가 지나면(같거나 넘으면) 다시 알린다.
 */
data class ResourceThresholds(
    val diskWarningPct: Double = 85.0,
    val diskCriticalPct: Double = 95.0,
    val diskResolveMarginPct: Double = 3.0,
    val diskConsecutive: Int = 2,
    val diskRecent: Duration = Duration.ofMinutes(2),
    val memoryPct: Double = 90.0,
    val memoryFor: Duration = Duration.ofMinutes(5),
    val memoryResolvePct: Double = 85.0,
    val memoryResolveFor: Duration = Duration.ofMinutes(5),
    val cpuPct: Double = 90.0,
    val cpuFor: Duration = Duration.ofMinutes(10),
    val cpuResolvePct: Double = 80.0,
    val cpuResolveFor: Duration = Duration.ofMinutes(5),
    val restartIncrease: Int = 3,
    val restartWindow: Duration = Duration.ofMinutes(10),
    val silentAfter: Duration = Duration.ofMinutes(3),
    val remindEvery: Duration = Duration.ofHours(1),
    /** agent 의 수집 간격 — 창의 기대 표본 수(창 ÷ 간격)와 "창을 덮었는가"의 기준 */
    val sampleInterval: Duration = Duration.ofSeconds(30),
) {
    init {
        // 수치가 말이 안 되면 뜰 때 실패한다 — 잘못된 설정으로 조용히 울리지 않거나 늘 울리지 않게
        for ((name, percent) in listOf("disk-warning-pct" to diskWarningPct, "disk-critical-pct" to diskCriticalPct, "memory-pct" to memoryPct,
                "memory-resolve-pct" to memoryResolvePct, "cpu-pct" to cpuPct, "cpu-resolve-pct" to cpuResolvePct)) {
            require(percent in 0.0..100.0) { "$name 은 0~100 이어야 한다: $percent" }
        }
        require(diskCriticalPct > diskWarningPct) { "disk-critical-pct 는 disk-warning-pct 보다 커야 한다" }
        require(diskResolveMarginPct >= 0 && diskResolveMarginPct < diskWarningPct) { "disk-resolve-margin-pct 는 0 이상, 경고 기준보다 작아야 한다" }
        require(memoryResolvePct <= memoryPct) { "memory-resolve-pct 는 memory-pct 보다 클 수 없다" }
        require(cpuResolvePct <= cpuPct) { "cpu-resolve-pct 는 cpu-pct 보다 클 수 없다" }
        require(diskConsecutive >= 1) { "disk-consecutive 는 1 이상" }
        require(restartIncrease >= 1) { "restart-increase 는 1 이상" }
        for ((name, duration) in listOf("disk-recent" to diskRecent, "memory-for" to memoryFor, "memory-resolve-for" to memoryResolveFor,
                "cpu-for" to cpuFor, "cpu-resolve-for" to cpuResolveFor, "restart-window" to restartWindow, "silent-after" to silentAfter,
                "remind-every" to remindEvery, "sample-interval" to sampleInterval)) {
            require(!duration.isNegative && !duration.isZero) { "$name 은 0 보다 길어야 한다" }
        }
    }
}

/**
 * 규칙 하나의 판정 상태(DB 에 둔다 — §2.4). [target] 은 재시작 규칙이면 컨테이너 ID, 그 밖은 빈 글자.
 * [targetName] 은 재시작 규칙의 컨테이너 실제 이름(알림 key · `firing[].target`). [since] 는 넘음으로 바뀐 판정 시각,
 * [lastNotifiedAt] 은 마지막으로 알림을 만든(보낼 표에 넣은) 시각, [lastValue] 는 마지막 판정의 값.
 */
data class RuleState(
    val server: String,
    val rule: ResourceRule,
    val target: String,
    val targetName: String?,
    val firing: Boolean,
    val since: Instant?,
    val lastNotifiedAt: Instant?,
    val lastValue: Double?,
)

/**
 * 보낼 알림 하나. [eventId] 는 이 알림의 고유 키 — 같은 (key, 판정 시각)이면 같은 값이라 같은 판정이 두 번 기록돼도 한 건이다.
 * [key] 는 알림 서버의 key: 넘음 · 다시 알림은 `resource:<서버>:<규칙>[:<컨테이너 이름>]`, 해소는 그 뒤에 `:resolved`.
 * 알림 서버는 key 를 120자에서 자르므로 key 는 120자를 넘지 않는다(긴 이름은 구현이 줄이되 넘음과 해소의 key 가 같아지면 안 된다).
 */
data class ResourceAlert(
    val eventId: String,
    val key: String,
    val kind: String,
    val severity: AlertSeverity,
    val title: String,
    val detail: String,
    val server: String,
    val rule: AlertRule,
    val targetName: String?,
    val createdAt: Instant,
) {
    /** 알림 서버 `POST /events` 의 본문 — `{source:"observe", kind, severity, key, title(1~200자), detail(1500자 이하)}` */
    fun eventBody(): Map<String, String> = mapOf(
        "source" to "observe", "kind" to kind, "severity" to severity.wireName, "key" to key,
        "title" to title.take(MAX_TITLE), "detail" to detail.take(MAX_DETAIL),
    )

    companion object {
        const val MAX_TITLE = 200
        const val MAX_DETAIL = 1500
    }
}

/** 해소 알림의 key 에 붙는 꼬리 */
internal const val RESOLVED_SUFFIX = ":resolved"

/** 알림 서버가 key 를 자르는 길이 */
private const val MAX_ALERT_KEY = 120

/** 글의 UTF-8 SHA-256 앞 12자(소문자 16진) — key 를 가르는 꼬리 */
internal fun shortHash(text: String): String =
    MessageDigest.getInstance("SHA-256").digest(text.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }.take(12)

/**
 * 넘음 key 를 뒤에 [RESOLVED_SUFFIX] 가 붙어도 120자를 넘지 않게 — 길면 앞부분과 전체의 해시로 줄인다(같은 [full] 은 늘 같은 key,
 * 다른 [full] 은 해시가 갈라 다른 key). 앞부분을 자르는 길은 [prefix](기본은 UTF-16 단위의 `take` — 자원 알림의 key 는 이 꼴 그대로)
 */
internal fun boundedBaseKey(full: String, prefix: (String, Int) -> String = { text, length -> text.take(length) }): String {
    val maxBaseKey = MAX_ALERT_KEY - RESOLVED_SUFFIX.length
    if (full.length <= maxBaseKey) return full
    val digest = shortHash(full)
    return prefix(full, maxBaseKey - digest.length - 1) + "~" + digest
}

/**
 * 판정의 입력 — [samples] 는 측정 시각이 `(now − lookback, now]` 인 항목(서버 섞임), [lastReceivedAt] 은 보관 중인 원본에서 본
 * 서버별 마지막 수신 시각, [expectedServers] 는 `METRICS_EXPECTED_SERVERS`.
 */
data class JudgeInput(
    val now: Instant,
    val samples: List<ResourceSample>,
    val lastReceivedAt: Map<String, Instant>,
    val expectedServers: Set<String>,
    /**
     * 끊김 규칙을 판정하는가(§15.1 — [SilentGate]). false 면 끊김은 보류다: 새로 울리지도, 울리던 것을 해소하지도, 다시 알리지도 않는다.
     * 다른 규칙은 그대로 판정한다
     */
    val silentJudging: Boolean = true,
)

/** 판정 결과 — [states] 는 판정 뒤의 **모든** 상태(앞 상태 중 이번에 보지 않은 것도 그대로 싣는다), [events] 는 이번에 만든 알림 */
data class JudgeResult(val states: List<RuleState>, val events: List<ResourceAlert>)

/**
 * 판정(순수 함수 — 시각은 [JudgeInput.now]). 저장된 최근 표본으로 **지금의 상태**만 계산하고, 앞 상태와 달라졌을 때(넘음 · 해소 · 디스크 단계)와
 * 넘어 있는 채로 [ResourceThresholds.remindEvery] 가 지났을 때만 알림을 만든다 — 판정이 오래 멈췄다 돌아와도 지난 구간을 되짚지 않는다.
 * 창 안의 표본(null 제외)이 기대 수(창 ÷ 간격)의 절반에 못 미치거나 창을 덮지 못하면(가장 오랜 표본이 `now − 창 + 간격` 보다 늦음)
 * 그 규칙은 판정을 미루고 상태를 그대로 둔다(결측은 넘음도 해소도 아니다). 재시작 넘음은 창을 덮지 않아도 되지만 절반 규칙은 따른다.
 * 넘어 있는 재시작 규칙의 컨테이너 ID 가 창 안에 하나도 없고 그 서버의 표본은 절반 이상이면(새 컨테이너로 바뀜) 해소다.
 * §16.2: "그 ID 가 없음"과 재시작 횟수의 증가량은 **컨테이너 갈래가 온전한 표본만**(`missing` 에 `containers` · `containers_truncated`
 * 가 없음) 보고 센다 — 잘린 표본의 값은 넘음 · 해소 어느 쪽도 좌우하지 않는다. 해소의 까닭은 해소 알림의 글(title · detail)로 가른다:
 * 컨테이너가 바뀌어 해소하면 그 글에 "바뀜"이 있고, 횟수가 늘지 않아 해소한 글과 다르다(해소된 상태 줄은 남기지 않는다 — 배포마다 쌓이지 않게).
 */
class ThresholdJudge(private val thresholds: ResourceThresholds = ResourceThresholds()) {
    /** 판정에 필요한 가장 긴 창 — 입력 표본은 `(now − lookback, now]` */
    val lookback: Duration
        get() = with(thresholds) { listOf(diskRecent, memoryFor, memoryResolveFor, cpuFor, cpuResolveFor, restartWindow).max() }

    fun judge(input: JudgeInput, previous: List<RuleState>): JudgeResult = Round(input).apply { run(previous) }.result()

    /** 값 하나의 표본 — 측정 시각과 값 */
    private data class Reading(val at: Instant, val value: Double)

    /** 규칙 하나의 대상(상태의 자리) */
    private data class Slot(val server: String, val rule: ResourceRule, val target: String)

    /** 판정 한 번 — 앞 상태를 고쳐 쓰며 알림을 모은다 */
    private inner class Round(private val input: JudgeInput) {
        private val now = input.now
        private val states = LinkedHashMap<Slot, RuleState>()
        private val events = mutableListOf<ResourceAlert>()

        fun run(previous: List<RuleState>) {
            previous.forEach { states[Slot(it.server, it.rule, it.target)] = it }
            val windowStart = now.minus(lookback)
            val byServer = input.samples.filter { it.measuredAt.isAfter(windowStart) && !it.measuredAt.isAfter(now) }
                .sortedBy { it.measuredAt }.groupBy { it.server }
            for (server in byServer.keys.sorted()) {
                val samples = byServer.getValue(server)
                judgeDisk(server, samples)
                judgeLevel(server, samples, ResourceRule.MEMORY, { memoryPct(it) }, thresholds.memoryPct, thresholds.memoryFor,
                    thresholds.memoryResolvePct, thresholds.memoryResolveFor, "메모리")
                judgeLevel(server, samples, ResourceRule.CPU, { it.host?.cpuPct }, thresholds.cpuPct, thresholds.cpuFor,
                    thresholds.cpuResolvePct, thresholds.cpuResolveFor, "CPU")
                judgeRestarts(server, samples)
            }
            // 끊김을 판정하지 않는 동안(§15.1 — 막 뜬 직후)은 끊김 규칙 전체를 보류한다: 울리지도, 해소하지도, 다시 알리지도 않는다
            if (input.silentJudging) for (server in (input.expectedServers + input.lastReceivedAt.keys).sorted()) judgeSilent(server)
        }

        fun result() = JudgeResult(states.values.toList(), events)

        // ── 공통 ──────────────────────────────────────────

        private fun firingState(server: String, rule: ResourceRule, target: String = ""): RuleState? =
            states[Slot(server, rule, target)]?.takeIf { it.firing }

        private fun readings(samples: List<ResourceSample>, window: Duration, value: (ResourceSample) -> Double?): List<Reading> {
            val start = now.minus(window)
            return samples.filter { it.measuredAt.isAfter(start) }.mapNotNull { sample -> value(sample)?.let { Reading(sample.measuredAt, it) } }
        }

        /** 창 안의 값이 기대 수(창 ÷ 간격)의 절반 이상인가 */
        private fun enough(count: Int, window: Duration) = count.toLong() * 2 >= window.toMillis() / thresholds.sampleInterval.toMillis()

        /** 절반 이상이고 가장 오래된 값이 창의 시작에서 한 간격 안인가(§14 — 막 들어오기 시작한 서버가 일찍 울리지 않게) */
        private fun covers(readings: List<Reading>, window: Duration) =
            enough(readings.size, window) && !readings.first().at.isAfter(now.minus(window).plus(thresholds.sampleInterval))

        private fun fire(server: String, rule: ResourceRule, value: Double?, title: String, detail: String, target: String = "",
                         targetName: String? = null, since: Instant = now) {
            states[Slot(server, rule, target)] = RuleState(server, rule, target, targetName, true, since, now, value)
            events += alert(server, rule, target, targetName, resolved = false, severityOf(rule), title, detail)
        }

        private fun resolve(state: RuleState, value: Double?, title: String, detail: String, severity: AlertSeverity = AlertSeverity.INFO) {
            states.remove(Slot(state.server, state.rule, state.target))
            events += alert(state.server, state.rule, state.target, state.targetName, resolved = true, severity, title, detail)
        }

        /** 넘음이 이어진다 — 마지막 알림에서 [ResourceThresholds.remindEvery] 가 지났으면 같은 key 로 다시 알린다(시작 시각은 그대로) */
        private fun keep(state: RuleState, value: Double?, title: String, detail: String) {
            val remind = state.lastNotifiedAt == null || Duration.between(state.lastNotifiedAt, now) >= thresholds.remindEvery
            states[Slot(state.server, state.rule, state.target)] = state.copy(lastValue = value, lastNotifiedAt = if (remind) now else state.lastNotifiedAt)
            if (remind) events += alert(state.server, state.rule, state.target, state.targetName, resolved = false, severityOf(state.rule),
                "(계속) $title", "${detail}\n처음 넘은 시각: ${state.since}")
        }

        private fun severityOf(rule: ResourceRule) = if (rule == ResourceRule.DISK_CRITICAL) AlertSeverity.CRITICAL else AlertSeverity.WARNING

        private fun alert(server: String, rule: ResourceRule, target: String, targetName: String?, resolved: Boolean, severity: AlertSeverity,
                          title: String, detail: String): ResourceAlert {
            val base = baseKey(server, rule, targetName)
            val key = if (resolved) "$base$RESOLVED_SUFFIX" else base
            // 같은 (key, 판정 시각)이면 같은 고유 키 — 재시작 규칙은 이름이 같은 두 컨테이너가 겹치지 않게 ID 를 붙인다
            val eventId = "$key@${now.toEpochMilli()}" + if (target.isEmpty()) "" else "#$target"
            return ResourceAlert(eventId, key, rule.kind, severity, title.take(ResourceAlert.MAX_TITLE), detail.take(ResourceAlert.MAX_DETAIL),
                server, rule, targetName, now)
        }

        // ── 디스크(세 단계) ────────────────────────────────

        private fun judgeDisk(server: String, samples: List<ResourceSample>) {
            val recent = readings(samples, thresholds.diskRecent) { diskPct(it) }.takeLast(thresholds.diskConsecutive)
            if (recent.size < thresholds.diskConsecutive) return                     // 결측 — 미룬다
            val values = recent.map { it.value }
            val warning = firingState(server, ResourceRule.DISK_WARNING)
            val critical = firingState(server, ResourceRule.DISK_CRITICAL)
            val belowWarning = values.all { it < thresholds.diskWarningPct - thresholds.diskResolveMarginPct }
            val latest = values.last()
            val shown = "디스크 ${percent(latest)}%"
            val detail = "최근 ${values.size}회: ${values.joinToString(" · ") { percent(it) + "%" }}"
            when {
                critical != null -> when {
                    belowWarning -> resolve(critical, latest, "$server 디스크 위험 해소 — $shown", detail)
                    values.all { it < thresholds.diskCriticalPct - thresholds.diskResolveMarginPct } -> {
                        resolve(critical, latest, "$server 디스크 위험 해소, 경고 유지 — $shown", detail, AlertSeverity.WARNING)
                        states[Slot(server, ResourceRule.DISK_WARNING, "")] =
                            RuleState(server, ResourceRule.DISK_WARNING, "", null, true, critical.since ?: now, now, latest)
                    }
                    else -> keep(critical, latest, "$server $shown — ${percent(thresholds.diskCriticalPct)}% 이상", detail)
                }
                values.all { it >= thresholds.diskCriticalPct } -> {
                    if (warning != null) states.remove(Slot(server, ResourceRule.DISK_WARNING, ""))
                    fire(server, ResourceRule.DISK_CRITICAL, latest, "$server $shown — ${percent(thresholds.diskCriticalPct)}% 이상(위험)", detail)
                }
                warning != null ->
                    if (belowWarning) resolve(warning, latest, "$server 디스크 경고 해소 — $shown", detail)
                    else keep(warning, latest, "$server $shown — ${percent(thresholds.diskWarningPct)}% 이상", detail)
                values.all { it >= thresholds.diskWarningPct } ->
                    fire(server, ResourceRule.DISK_WARNING, latest, "$server $shown — ${percent(thresholds.diskWarningPct)}% 이상(경고)", detail)
            }
        }

        // ── 메모리 · CPU(기준이 창 동안 이어짐) ───────────────

        private fun judgeLevel(server: String, samples: List<ResourceSample>, rule: ResourceRule, value: (ResourceSample) -> Double?,
                               firePct: Double, fireFor: Duration, resolvePct: Double, resolveFor: Duration, label: String) {
            val current = firingState(server, rule)
            if (current == null) {
                val window = readings(samples, fireFor, value)
                if (window.isEmpty() || !covers(window, fireFor)) return               // 결측 · 창을 덮지 못함 — 미룬다
                if (window.all { it.value >= firePct }) {
                    val latest = window.last().value
                    fire(server, rule, latest, "$server $label ${percent(latest)}% — ${minutes(fireFor)} 넘게 ${percent(firePct)}% 이상",
                        "최근 ${minutes(fireFor)} 의 최저 ${percent(window.minOf { it.value })}% · 최고 ${percent(window.maxOf { it.value })}% (표본 ${window.size}개)")
                }
            } else {
                val window = readings(samples, resolveFor, value)
                if (window.isEmpty() || !covers(window, resolveFor)) return
                val latest = window.last().value
                val detail = "최근 ${minutes(resolveFor)} 의 최저 ${percent(window.minOf { it.value })}% · 최고 ${percent(window.maxOf { it.value })}% (표본 ${window.size}개)"
                if (window.all { it.value < resolvePct }) resolve(current, latest, "$server $label 해소 — ${percent(latest)}%, ${minutes(resolveFor)} 동안 ${percent(resolvePct)}% 아래", detail)
                else keep(current, latest, "$server $label ${percent(latest)}% — ${percent(firePct)}% 이상이 이어짐", detail)
            }
        }

        // ── 컨테이너 재시작(ID 별) ─────────────────────────

        private fun judgeRestarts(server: String, samples: List<ResourceSample>) {
            val window = thresholds.restartWindow
            val start = now.minus(window)
            val inWindow = samples.filter { it.measuredAt.isAfter(start) }
            // 컨테이너 갈래가 온전한 표본(읽지 못했거나 잘린 것이 아님)만 센다(§16.2) — "그 ID 가 사라짐"도, 재시작 횟수의 증가량도.
            // 잘린 표본의 값은 넘음 · 해소 어느 쪽도 좌우하지 않는다
            val intact = inWindow.filter { sample -> sample.missing.none { it in INCOMPLETE_CONTAINERS } }
            val seenIds = intact.flatMapTo(HashSet()) { sample -> sample.containers.map { it.id } }
            // 컨테이너 ID 마다 (재시작 횟수, 이름) — 새 ID 는 새 기준(배포는 재시작이 아니다)
            val byContainer = LinkedHashMap<String, MutableList<Pair<Long, String>>>()
            for (sample in intact) for (container in sample.containers) {
                val restarts = container.restarts ?: continue
                byContainer.getOrPut(container.id) { mutableListOf() } += restarts to container.name
            }
            val firing = states.values.filter { it.server == server && it.rule == ResourceRule.RESTARTS && it.firing }
            for (state in firing) {
                if (state.target !in seenIds) {
                    // 울리던 컨테이너가 온전한 표본에서 사라지고 서버는 계속 보낸다 — 새 컨테이너로 바뀌었다(§14). 온전한 표본이 절반 이상이고
                    // 가장 최근 표본도 온전할 때만 — 컨테이너를 읽지 못한 것은 넘음도 해소도 아니다(보류). 해소 글에 까닭("바뀜")을 적는다
                    if (enough(intact.size, window) && inWindow.lastOrNull()?.missing?.none { it in INCOMPLETE_CONTAINERS } == true) {
                        resolve(state, null, "$server 컨테이너 ${state.targetName} 재시작 해소 — 컨테이너가 바뀜",
                            "최근 ${minutes(window)} 에 그 컨테이너(${state.target})가 보이지 않는다")
                    }
                    continue
                }
                val counts = byContainer[state.target] ?: continue            // 보이지만 재시작 횟수를 읽지 못했다 — 보류
                if (!enough(counts.size, window)) continue
                val increase = (counts.last().first - counts.first().first).toDouble()
                if (increase <= 0) resolve(state, 0.0, "$server 컨테이너 ${state.targetName} 재시작 해소 — 재시작이 멈춤", "최근 ${minutes(window)} 동안 재시작이 늘지 않았다")
                else keep(state, increase, "$server 컨테이너 ${state.targetName} 재시작 반복 — ${minutes(window)}에 +${increase.toLong()}",
                    "재시작 횟수 ${counts.first().first} → ${counts.last().first}")
            }
            for ((containerId, counts) in byContainer) {
                if (firingState(server, ResourceRule.RESTARTS, containerId) != null || !enough(counts.size, window)) continue
                val increase = counts.last().first - counts.first().first
                if (increase >= thresholds.restartIncrease) {
                    val name = counts.last().second
                    fire(server, ResourceRule.RESTARTS, increase.toDouble(), "$server 컨테이너 $name 재시작 반복 — ${minutes(window)}에 +$increase",
                        "재시작 횟수 ${counts.first().first} → ${counts.last().first} (컨테이너 $containerId)", target = containerId, targetName = name)
                }
            }
        }

        // ── 지표 끊김(수신 시각) ──────────────────────────

        private fun judgeSilent(server: String) {
            val received = input.lastReceivedAt[server]
            val age = received?.let { Duration.between(it, now) }
            val silent = age == null || age > thresholds.silentAfter                 // 정확히 3분은 끊김이 아니다
            val ageMinutes = age?.let { it.toMillis() / 60_000.0 }
            val shown = if (received == null) "수신 기록 없음" else "마지막 수신 $received (${percent(ageMinutes!!)}분 전)"
            val current = firingState(server, ResourceRule.SILENT)
            when {
                current != null && !silent -> resolve(current, ageMinutes, "$server 지표 다시 들어옴", shown)
                current != null -> keep(current, ageMinutes, "$server 지표 끊김 — ${minutes(thresholds.silentAfter)} 넘게 안 들어옴", shown)
                silent -> fire(server, ResourceRule.SILENT, ageMinutes, "$server 지표 끊김 — ${minutes(thresholds.silentAfter)} 넘게 안 들어옴", shown)
            }
        }
    }

    private companion object {
        /** 이 낱말이 `missing` 에 있으면 그 표본의 컨테이너 목록은 온전하지 않다 */
        val INCOMPLETE_CONTAINERS = setOf("containers", "containers_truncated")

        /** `resource:<서버>:<규칙>[:<컨테이너 이름>]` — 길면 줄인다([boundedBaseKey]) */
        fun baseKey(server: String, rule: ResourceRule, targetName: String?): String =
            boundedBaseKey("resource:$server:${rule.wireName}" + (targetName?.let { ":$it" } ?: ""))

        fun memoryPct(sample: ResourceSample): Double? = ratio(sample.host?.memUsed, sample.host?.memTotal)
        fun diskPct(sample: ResourceSample): Double? = ratio(sample.host?.diskUsed, sample.host?.diskTotal)
        private fun ratio(used: Long?, total: Long?): Double? = if (used == null || total == null || total <= 0) null else used * 100.0 / total

        fun percent(value: Double) = "%.1f".format(value)
        fun minutes(duration: Duration) = if (duration.toSeconds() % 60 == 0L) "${duration.toMinutes()}분" else "${duration.toSeconds()}초"
    }
}
