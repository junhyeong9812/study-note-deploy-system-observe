package xyz.junproject.observe.metric

import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/**
 * 지표 스트림(`metrics`)의 항목 하나 — 원문 그대로의 칸(이름 → 값). 명세 §10.2 의 칸은 `kind` · `v` · `server` · `at` · `body`.
 * 확인하지 못한 채 스트림 상한에 밀려난 항목은 ID 만 오고 칸이 비어 있다.
 */
data class MetricStreamRecord(val id: String, val fields: Map<String, String>)

/**
 * 지표의 갈래와 그 갈래의 값 이름 — 값 이름은 스트림 `body` 의 칸 이름이자 조회 응답의 칸 이름이다
 * (`GET /metrics/series` 의 점은 값마다 `<이름>`(평균) 과 `<이름>_max`(최댓값)).
 * 컨테이너 계열은 CPU · 메모리만 그래프 값이다(재시작 횟수 · 상태는 `GET /metrics/latest` 에만).
 */
enum class MetricScope(val wireName: String, val values: List<String>) {
    HOST("host", listOf("cpu_pct", "load1", "mem_total", "mem_used", "disk_total", "disk_used", "net_rx_bps", "net_tx_bps", "io_read_bps", "io_write_bps")),
    CONTAINER("container", listOf("cpu_pct", "mem_used")),
    GPU("gpu", listOf("util_pct", "mem_used", "mem_total", "temp_c"));

    companion object {
        fun of(wireName: String): MetricScope? = entries.firstOrNull { it.wireName == wireName }
    }
}

/** 서버 전체 값 — 읽지 못했거나 앞 수집과의 차이를 낼 수 없는 값은 null(§10.2). 바이트 수는 정수, 사용률·초당 값·부하는 실수 */
data class HostValues(
    val cpuPct: Double? = null,
    val load1: Double? = null,
    val memTotal: Long? = null,
    val memUsed: Long? = null,
    val diskTotal: Long? = null,
    val diskUsed: Long? = null,
    val netRxBps: Double? = null,
    val netTxBps: Double? = null,
    val ioReadBps: Double? = null,
    val ioWriteBps: Double? = null,
)

/** 컨테이너 하나 — [name] 은 실제 이름(`front-app-green`), 계열 이름은 [ContainerName.seriesName] */
data class ContainerValues(
    val name: String,
    val id: String,
    val project: String,
    val cpuPct: Double? = null,
    val memUsed: Long? = null,
    val restarts: Long? = null,
    val state: String? = null,
)

/** GPU 하나 */
data class GpuValues(
    val index: Int,
    val utilPct: Double? = null,
    val memUsed: Long? = null,
    val memTotal: Long? = null,
    val tempC: Double? = null,
)

/**
 * 저장하는 지표 한 묶음(스트림 항목 하나 = 한 서버의 한 번 수집). [id] 가 고유 키(같은 항목을 두 번 받아도 한 번만 저장).
 * [measuredAt] 은 agent 의 시계(`at`), [receivedAt] 은 관측 서버가 받은 시각 — 둘 다 저장한다(§11.2).
 * [host] 는 `body` 에 `host` 가 없거나 null 이면 null. [missing] 은 받은 그대로.
 */
data class ResourceSample(
    val id: StreamId,
    val server: String,
    val measuredAt: Instant,
    val receivedAt: Instant,
    val intervalMs: Long?,
    val host: HostValues?,
    val containers: List<ContainerValues>,
    val gpus: List<GpuValues>,
    val missing: List<String>,
)

/** 버린 항목의 이유 — 헬스의 `metrics.dropped` 에 나가는 글자 그대로 */
enum class MetricDropReason(val wireName: String) {
    /** 스트림 ID 가 `<밀리초>-<순번>` 꼴이 아니다 */
    INVALID_ID("invalid_id"),
    /** 칸이 하나도 없다 — 확인하지 못한 채 스트림 상한에 밀려났다 */
    TRIMMED("trimmed"),
    /** `kind` · `v` · `server` · `at` · `body` 중 빠진 칸(빈 글자 포함)이 있다 */
    MISSING_FIELD("missing_field"),
    /** `kind` 가 `resource` 인데 `v` 가 `1` 이 아니다 */
    UNSUPPORTED_VERSION("unsupported_version"),
    /** 서버 이름 꼴이 아니다([MetricEntryParser.SERVER_NAME]) */
    INVALID_SERVER("invalid_server"),
    /** `at` 이 epoch 밀리초(양의 정수)가 아니다 */
    INVALID_AT("invalid_at"),
    /** 측정 시각이 수신 시각보다 [MetricEntryParser.MAX_CLOCK_AHEAD] 넘게 앞선다(서버 시계가 빠름) */
    CLOCK_AHEAD("clock_ahead"),
    /** `body` 가 [MetricEntryParser.MAX_BODY_BYTES] 바이트(UTF-8)를 넘는다 */
    BODY_TOO_LARGE("body_too_large"),
    /** `body` 가 JSON 이 아니다(NaN 같은 JSON 밖의 낱말 포함) */
    BODY_NOT_JSON("body_not_json"),
    /** `body` 가 JSON 객체가 아니거나 갈래의 꼴이 다르다(`host` 가 객체가 아님 · `containers` 가 배열이 아님 · 컨테이너 이름·ID 꼴 · GPU 번호 등) */
    INVALID_BODY("invalid_body"),
    /**
     * 값이 숫자·null 이 아니거나(글자 "3"·"NaN"·true 포함), 범위 밖이다 — 모든 값은 0 이상, 서버 CPU·GPU 사용률은 0~100,
     * GPU 온도는 −50~150, 바이트 수·재시작 횟수·`interval_ms` 는 정수(컨테이너 CPU 는 코어 하나 = 100 이라 위 상한이 없다)
     */
    INVALID_VALUE("invalid_value"),
}

/** 스트림 항목 하나를 해석한 결과 */
sealed interface MetricEntryParse {
    /** 저장할 묶음 */
    data class Accepted(val sample: ResourceSample) : MetricEntryParse
    /** 자원 지표가 아닌 다른 종류(§4 — 같은 길을 다른 종류가 쓸 수 있다). 버린 것으로 세지 않고, 저장하지 않고, 확인한다 */
    data class OtherKind(val kind: String) : MetricEntryParse
    /** 꼴이 틀린 항목 — 이유를 세고 확인한다(다시 받지 않는다) */
    data class Dropped(val reason: MetricDropReason) : MetricEntryParse
}

/**
 * 스트림 항목의 해석(순수 함수). 검사 순서: ID → 칸 없음(TRIMMED) → `kind`(없으면 MISSING_FIELD, `resource` 가 아니면 OtherKind —
 * 다른 종류의 나머지 칸은 보지 않는다) → `v` → `server` → `at` → 시계 앞섬 → `body` 크기 → JSON → 꼴 → 값.
 * 값 하나라도 틀리면 그 항목 전체를 버린다(그 항목만 — 같은 묶음의 다른 항목은 저장한다).
 */
object MetricEntryParser {
    const val KIND = "resource"
    const val VERSION = "1"
    const val MAX_BODY_BYTES = 64 * 1024
    val MAX_CLOCK_AHEAD: Duration = Duration.ofMinutes(2)
    /** agent 의 `SERVER_NAME`(예: `ci-agent-158`) — 조회의 `server` 도 같은 꼴 */
    val SERVER_NAME = Regex("^[a-z0-9][a-z0-9._-]{0,62}$")

    /** [receivedAt] 은 관측 서버가 이 항목을 받은 시각 — 시계 앞섬 판정과 저장의 수신 시각 */
    fun parse(record: MetricStreamRecord, receivedAt: Instant): MetricEntryParse = TODO("not implemented")
}

/** 컨테이너의 계열 이름 — 자리 이름(`-blue` · `-green`)을 뗀 이름(§11.5) */
object ContainerName {
    /** 끝의 `-blue` · `-green` 하나만 뗀다. 떼면 빈 이름이 되거나 그 꼴로 끝나지 않으면 그대로 */
    fun seriesName(actualName: String): String = TODO("not implemented")
}
