package xyz.junproject.observe.threshold

import xyz.junproject.observe.metric.ResourceSample
import java.time.Duration
import java.time.Instant

/**
 * 임계값 규칙(명세 §2.4 · §11.4). [wireName] 은 알림 key 의 `<규칙>` 자리 · `GET /metrics/latest` 의 `firing[].rule` 글자,
 * [kind] 는 알림 서버 `POST /events` 의 `kind`. 디스크는 단계마다 규칙 하나(경고 · 위험 — 같은 key 를 알림 서버가 10분 억제하므로
 * 단계가 바뀐 알림이 억제되지 않게 key 를 나눈다).
 */
enum class ResourceRule(val wireName: String, val kind: String) {
    DISK_WARNING("disk_warning", "resource_disk"),
    DISK_CRITICAL("disk_critical", "resource_disk"),
    MEMORY("memory", "resource_memory"),
    CPU("cpu", "resource_cpu"),
    RESTARTS("restarts", "resource_restarts"),
    SILENT("silent", "resource_silent"),
}

/** 알림 서버의 `severity` 글자(알림 서버가 받는 셋 — info · warning · critical) */
enum class AlertSeverity(val wireName: String) { INFO("info"), WARNING("warning"), CRITICAL("critical") }

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
        // 수치가 말이 안 되면 뜰 때 실패한다(구현 전 — 규칙은 ThresholdJudgeTest 가 고정한다)
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
    val rule: ResourceRule,
    val targetName: String?,
    val createdAt: Instant,
) {
    /** 알림 서버 `POST /events` 의 본문 — `{source:"observe", kind, severity, key, title(1~200자), detail(1500자 이하)}` */
    fun eventBody(): Map<String, String> = TODO("not implemented")
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
)

/** 판정 결과 — [states] 는 판정 뒤의 **모든** 상태(앞 상태 중 이번에 보지 않은 것도 그대로 싣는다), [events] 는 이번에 만든 알림 */
data class JudgeResult(val states: List<RuleState>, val events: List<ResourceAlert>)

/**
 * 판정(순수 함수 — 시각은 [JudgeInput.now]). 저장된 최근 표본으로 **지금의 상태**만 계산하고, 앞 상태와 달라졌을 때(넘음 · 해소 · 디스크 단계)와
 * 넘어 있는 채로 [ResourceThresholds.remindEvery] 가 지났을 때만 알림을 만든다 — 판정이 오래 멈췄다 돌아와도 지난 구간을 되짚지 않는다.
 * 창 안의 표본(null 제외)이 기대 수(창 ÷ 간격)의 절반에 못 미치거나 창을 덮지 못하면(가장 오랜 표본이 `now − 창 + 간격` 보다 늦음)
 * 그 규칙은 판정을 미루고 상태를 그대로 둔다(결측은 넘음도 해소도 아니다). 재시작 넘음은 창을 덮지 않아도 되지만 절반 규칙은 따른다.
 * 넘어 있는 재시작 규칙의 컨테이너 ID 가 창 안에 하나도 없고 그 서버의 표본은 절반 이상이면(새 컨테이너로 바뀜) 해소다.
 */
class ThresholdJudge(private val thresholds: ResourceThresholds = ResourceThresholds()) {
    /** 판정에 필요한 가장 긴 창 — 입력 표본은 `(now − lookback, now]` */
    val lookback: Duration get() = TODO("not implemented")

    fun judge(input: JudgeInput, previous: List<RuleState>): JudgeResult = TODO("not implemented")
}
