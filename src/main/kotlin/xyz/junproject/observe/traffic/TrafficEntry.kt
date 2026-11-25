package xyz.junproject.observe.traffic

import xyz.junproject.observe.metric.MetricStreamRecord
import java.time.Duration
import java.time.Instant

/** 구간 묶음의 갈래(규약 3.6) — 글자는 항목의 `class` 그대로 */
enum class TrafficClass(val wireName: String) {
    REQUEST("request"), HEALTH("health"), STREAM("stream");

    companion object {
        fun of(wireName: String): TrafficClass? = entries.firstOrNull { it.wireName == wireName }
    }
}

/** 상태의 갈래별 수(규약 4.3 — 다섯 열쇠는 0 이어도 있다) */
data class TrafficStatusCounts(val status2xx: Long, val status3xx: Long, val status4xx: Long, val status5xx: Long, val other: Long) {
    /** 다섯 갈래의 합 — 뼈대: 구현 워커가 채운다 */
    val total: Long get() = TODO("뼈대: 구현 워커가 채운다")

    /** 갈래마다 더한다 — 뼈대: 구현 워커가 채운다 */
    operator fun plus(more: TrafficStatusCounts): TrafficStatusCounts = TODO("뼈대: 구현 워커가 채운다")

    companion object {
        val ZERO = TrafficStatusCounts(0, 0, 0, 0, 0)
    }
}

/**
 * 한 칸(또는 칸 여럿을 합친 것)의 수치 — 규약 4.2 의 칸끼리 더하는 법: `count` · `status` · `buckets` 는 칸마다 더하고 `max_ms` 는 큰 쪽.
 * [buckets] 는 늘 [TrafficDistribution.BUCKET_COUNT] 개. [maxMs] 는 `request` 갈래가 아니면 null.
 */
data class TrafficTotals(val count: Long, val status: TrafficStatusCounts, val buckets: List<Long>, val maxMs: Long?) {
    /** 칸끼리 더하기 — 뼈대: 구현 워커가 채운다 */
    operator fun plus(more: TrafficTotals): TrafficTotals = TODO("뼈대: 구현 워커가 채운다")

    companion object {
        val EMPTY = TrafficTotals(0, TrafficStatusCounts.ZERO, List(TrafficDistribution.BUCKET_COUNT) { 0L }, null)
    }
}

/** 항목 안의 구간 묶음 하나 — 열쇠 `(bucket_start_ms, service, caller, class)`, 서버는 항목의 `server` */
data class TrafficSegment(
    val bucketStart: Instant,
    val service: String,
    val caller: String,
    val trafficClass: TrafficClass,
    val totals: TrafficTotals,
)

/** 요청별 줄 하나(규약 4.2 `requests[]`) — 없음은 null */
data class TrafficRequest(
    val requestId: String,
    val service: String,
    val caller: String,
    val method: String,
    val path: String,
    val pathTruncated: Boolean,
    val status: Int,
    val endAt: Instant,
    val durationMs: Long,
    val upstreamMs: Long?,
    val upstreamStatus: String?,
    val slot: String?,
    val bytesSent: Long,
)

/**
 * 스트림 항목 하나 = 에이전트가 읽은 범위 하나(규약 4.2). 멱등 열쇠는 `(server, generation, sequence)`.
 * [readAt] 은 항목의 `at`(에이전트의 벽시계 — 버리는 데 쓰지 않는다), [receivedAt] 은 관측 서버가 받은 시각.
 */
data class TrafficEntry(
    val server: String,
    val generation: String,
    val sequence: Long,
    val readAt: Instant,
    val receivedAt: Instant,
    val segments: List<TrafficSegment>,
    val requests: List<TrafficRequest>,
    val requestsDropped: Long,
    val missing: List<String>,
    /** 경로에 쿼리(`?`)나 주소 꼴이 들어 있어 관측 서버가 그 줄만 버린 요청별 줄 수 */
    val requestsRejected: Long = 0,
)

/** 항목 전체를 버린 이유 — 헬스의 `traffic.dropped` 에 나가는 글자 그대로 */
enum class TrafficDropReason(val wireName: String) {
    /** 스트림 ID 가 `<밀리초>-<순번>` 꼴이 아니다 */
    INVALID_ID("invalid_id"),
    /** 칸이 하나도 없다 — 확인하지 못한 채 스트림 상한에 밀려났다 */
    TRIMMED("trimmed"),
    /** `kind` · `v` · `server` · `generation` · `seq` · `at` · `body` 중 빠진 칸(빈 글자 포함) */
    MISSING_FIELD("missing_field"),
    /** `v` 가 `1` 이 아니다 */
    UNSUPPORTED_VERSION("unsupported_version"),
    /** 서버 이름 꼴이 아니다 */
    INVALID_SERVER("invalid_server"),
    /** `generation` 이 16진수(소문자) 16자가 아니다 */
    INVALID_GENERATION("invalid_generation"),
    /** `seq` 가 1 이상의 10진 정수 글자(앞자리 0 없음)가 아니다 */
    INVALID_SEQ("invalid_seq"),
    /** `at` 이 양의 epoch 밀리초가 아니다 */
    INVALID_AT("invalid_at"),
    /** `body` 가 32768 바이트(UTF-8)를 넘는다 */
    BODY_TOO_LARGE("body_too_large"),
    /** `body` 가 JSON 이 아니다 */
    BODY_NOT_JSON("body_not_json"),
    /** `body` 의 꼴이 규약 4.2 · 4.3 과 다르다 */
    INVALID_BODY("invalid_body"),
}

/** 스트림 항목 하나를 해석한 결과 */
sealed interface TrafficEntryParse {
    data class Accepted(val entry: TrafficEntry) : TrafficEntryParse
    /** 통신 항목이 아닌 다른 종류 — 저장하지 않고, 버린 것으로 세지 않고, 확인한다 */
    data class OtherKind(val kind: String) : TrafficEntryParse
    data class Dropped(val reason: TrafficDropReason) : TrafficEntryParse
}

/**
 * 칸의 시각이 받은 시각보다 얼마나 앞서거나 뒤졌는가(규약 4.5) — 칸(`bucket_start_ms`)과 요청별 줄(`end_ms`)은 버리고 세며, 항목의 `at` 은 세기만 한다.
 * 뼈대: 본문은 구현 워커가 채운다.
 */
object TrafficCellAge {
    val MAX_AHEAD: Duration = Duration.ofMinutes(2)
    val MAX_LATENESS: Duration = Duration.ofDays(6)

    /** [at] 이 [receivedAt] 보다 2분 **넘게** 앞선다(정확히 2분은 아니다) */
    fun isAhead(at: Instant, receivedAt: Instant): Boolean = TODO("뼈대: 구현 워커가 채운다")

    /** [at] 이 [receivedAt] 보다 6일 **넘게** 뒤다(정확히 6일은 아니다) */
    fun isTooOld(at: Instant, receivedAt: Instant): Boolean = TODO("뼈대: 구현 워커가 채운다")
}

/**
 * 통신 스트림 항목의 해석(순수 함수 · 규약 4.2 · 4.3 · 7.2). 검사 순서: 스트림 ID → 칸 없음(`trimmed`) → `kind`(없으면 `missing_field`, `traffic` 이 아니면
 * [TrafficEntryParse.OtherKind]) → 빠진 칸(`v` · `server` · `generation` · `seq` · `at` · `body`, 빈 글자 포함) → `v` → `server` → `generation` → `seq` → `at` →
 * body 크기(32768 UTF-8 바이트까지 받는다) → JSON(객체 뒤에 더 붙은 글자도 거절) → 꼴. 모르는 칸은 무시한다(스트림 칸 · body · 묶음 · 요청별 줄 모두).
 * `at` 과 칸의 시각으로는 버리지 않는다 — 칸은 저장이 순번 다음에 거른다.
 *
 * 꼴(틀리면 `invalid_body`): body 는 객체이고 `sources`(객체의 배열) · `missing`([MISSING] 꼴 글자의 배열) · `segments` · `requests`(배열, [MAX_REQUESTS] 줄까지) ·
 * `requests_dropped`(0 이상 정수) · `dependencies`(배열 — 내용은 보지 않는다)가 모두 있다.
 * 묶음: `bucket_start_ms`(양의 정수, 30000 의 배수) · `service` · `caller`([NAME] 꼴 — 닫힌 목록이 아니다) · `class`(request · health · stream) · `count`(1 이상) ·
 * `status`(다섯 열쇠 모두, 0 이상 정수, 합 = count) · `buckets`(0 이상 정수 20개) · `max_ms`(request 면 0 이상 정수, 아니면 null) — request 는 분포의 합 = count,
 * 아니면 분포가 모두 0. 한 항목 안에서 열쇠 `(bucket_start_ms, service, caller, class)` 는 겹치지 않는다.
 * 요청별 줄: `request_id`([REQUEST_ID]) · `service` · `caller`([NAME]) · `method`([METHOD]) · `path`(1024자까지, 제어 문자가 없다 — 규약 3.2)
 * — 경로에 `?` 가 있거나 주소 꼴(IPv4 점 넷 · IPv6 콜론 둘 이상의 16진 묶음, `%3A` · `%2E` 를 푼 뒤에도)이 들었으면 **그 줄만** 버리고
 * [TrafficEntry.requestsRejected] 에 센다(항목 · 묶음은 그대로 — 묶음에는 경로가 없다). 점 셋 미만(`/release/1.2.3`) · 콜론 하나(`/time/12:30`)는 주소가 아니다 ·
 * `path_truncated`(참 · 거짓) · `status`(0~999 정수) · `end_ms`(양의 정수) · `duration_ms` · `bytes_sent`(0 이상 정수) · `upstream_ms`(0 이상 정수 또는 null) ·
 * `upstream_status` · `slot`(128자까지의 글자 또는 null). 칸은 생략하지 않는다(규약 4.3) — 빠지면 꼴이 틀린 것.
 * 뼈대: 본문은 구현 워커가 채운다.
 */
object TrafficEntryParser {
    const val KIND = "traffic"
    const val VERSION = "1"
    const val MAX_BODY_BYTES = 32 * 1024
    const val MAX_REQUESTS = 100
    const val MAX_PATH_LENGTH = 1024
    const val CELL_MS = 30_000L
    val SERVER_NAME = Regex("^[a-z0-9][a-z0-9._-]{0,62}$")
    val GENERATION = Regex("^[0-9a-f]{16}$")
    val SEQUENCE = Regex("^[1-9][0-9]{0,17}$")
    /** 서비스 · 호출자의 꼴 — 닫힌 목록이 아니라 꼴만 본다(이름을 더하는 것은 호환 — 규약 7.2) */
    val NAME = Regex("^[a-z][a-z0-9-]{0,31}$")
    val REQUEST_ID = Regex("^[A-Za-z0-9_-]{1,64}$")
    val METHOD = Regex("^[A-Z_-]{0,32}$")
    val MISSING = Regex("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}:[a-z_]{1,32}$")

    /** [receivedAt] 은 관측 서버가 이 항목을 받은 시각 — [TrafficEntry.receivedAt] 에 그대로 */
    fun parse(record: MetricStreamRecord, receivedAt: Instant): TrafficEntryParse = TODO("뼈대: 구현 워커가 채운다")
}
