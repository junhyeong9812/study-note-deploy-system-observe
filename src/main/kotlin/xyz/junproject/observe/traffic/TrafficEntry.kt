package xyz.junproject.observe.traffic

import tools.jackson.core.JacksonException
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import xyz.junproject.observe.log.StreamId
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
    /** 다섯 갈래의 합 */
    val total: Long get() = status2xx + status3xx + status4xx + status5xx + other

    /** 갈래마다 더한다 */
    operator fun plus(more: TrafficStatusCounts) = TrafficStatusCounts(
        status2xx + more.status2xx, status3xx + more.status3xx, status4xx + more.status4xx, status5xx + more.status5xx, other + more.other,
    )

    companion object {
        val ZERO = TrafficStatusCounts(0, 0, 0, 0, 0)
    }
}

/**
 * 한 칸(또는 칸 여럿을 합친 것)의 수치 — 규약 4.2 의 칸끼리 더하는 법: `count` · `status` · `buckets` 는 칸마다 더하고 `max_ms` 는 큰 쪽.
 * [buckets] 는 늘 [TrafficDistribution.BUCKET_COUNT] 개. [maxMs] 는 `request` 갈래가 아니면 null.
 */
data class TrafficTotals(val count: Long, val status: TrafficStatusCounts, val buckets: List<Long>, val maxMs: Long?) {
    /** 칸끼리 더하기 */
    operator fun plus(more: TrafficTotals) = TrafficTotals(
        count + more.count, status + more.status, buckets.zip(more.buckets, Long::plus), listOfNotNull(maxMs, more.maxMs).maxOrNull(),
    )

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
 */
object TrafficCellAge {
    val MAX_AHEAD: Duration = Duration.ofMinutes(2)
    val MAX_LATENESS: Duration = Duration.ofDays(6)

    /** [at] 이 [receivedAt] 보다 2분 **넘게** 앞선다(정확히 2분은 아니다) */
    fun isAhead(at: Instant, receivedAt: Instant): Boolean = at.isAfter(receivedAt.plus(MAX_AHEAD))

    /** [at] 이 [receivedAt] 보다 6일 **넘게** 뒤다(정확히 6일은 아니다) */
    fun isTooOld(at: Instant, receivedAt: Instant): Boolean = at.isBefore(receivedAt.minus(MAX_LATENESS))
}

/**
 * 통신 스트림 항목의 해석(순수 함수 · 규약 4.2 · 4.3 · 7.2). 검사 순서: 스트림 ID → 칸 없음(`trimmed`) → `kind`(없으면 `missing_field`, `traffic` 이 아니면
 * [TrafficEntryParse.OtherKind]) → 빠진 칸(`v` · `server` · `generation` · `seq` · `at` · `body`, 빈 글자 포함) → `v` → `server` → `generation` → `seq` → `at` →
 * body 크기(32768 UTF-8 바이트까지 받는다) → JSON(객체 뒤에 더 붙은 글자도 거절) → 꼴. 모르는 칸은 무시한다(스트림 칸 · body · 묶음 · 요청별 줄 모두).
 * `at` 과 칸의 시각으로는 버리지 않는다 — 칸은 저장이 순번 다음에 거른다.
 *
 * 꼴(틀리면 `invalid_body`): body 는 객체이고 `sources`(객체의 배열) · `missing`([MISSING] 꼴 글자의 배열) · `segments` · `requests`(배열, [MAX_REQUESTS] 줄까지) ·
 * `requests_dropped`(0 이상 정수) · `dependencies`(배열 — 내용은 보지 않는다)가 모두 있다.
 * 묶음: `bucket_start_ms`(양의 정수, 30000 의 배수) · `service` · `caller`([NAME] 꼴 — 닫힌 목록이 아니다) · `class`(request · health · stream) · `count`(1 이상 [MAX_CELL_COUNT] 이하 — 분포의 각 값도 그 이하) ·
 * `status`(다섯 열쇠 모두, 0 이상 정수, 합 = count) · `buckets`(0 이상 정수 20개) · `max_ms`(request 면 0 이상 정수, 아니면 null) — request 는 분포의 합 = count,
 * 아니면 분포가 모두 0. 한 항목 안에서 열쇠 `(bucket_start_ms, service, caller, class)` 는 겹치지 않는다.
 * 요청별 줄: `request_id`([REQUEST_ID]) · `service` · `caller`([NAME]) · `method`([METHOD]) · `path`(1024자까지, 제어 문자가 없다 — 규약 3.2)
 * — 경로에 `?` · `%3F` 가 있거나 주소 꼴([isPathAllowed])이 들었으면 **그 줄만** 버리고
 * [TrafficEntry.requestsRejected] 에 센다(항목 · 묶음은 그대로 — 묶음에는 경로가 없다). 점 셋 미만(`/release/1.2.3`) · 콜론만 이은 낱말(`/time/12:30:00`)은 주소가 아니다 ·
 * `path_truncated`(참 · 거짓) · `status`(0~999 정수) · `end_ms`(양의 정수) · `duration_ms` · `bytes_sent`(0 이상 정수) · `upstream_ms`(0 이상 정수 또는 null) ·
 * `upstream_status` · `slot`(128자까지의 글자 또는 null). 칸은 생략하지 않는다(규약 4.3) — 빠지면 꼴이 틀린 것.
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

    private val AT = Regex("^[0-9]{1,15}$")
    private const val MAX_TEXT_LENGTH = 128
    private const val MAX_STATUS = 999
    private val STREAM_FIELDS = listOf("v", "server", "generation", "seq", "at", "body")
    private val STATUS_KEYS = listOf("2xx", "3xx", "4xx", "5xx", "other")

    /** 한 칸의 `count` 와 분포의 각 값의 상한 — 넘치는 수 하나가 5분 묶음의 합을 오류로 만들어 그 구간의 묶음 · 삭제를 막지 않게 */
    const val MAX_CELL_COUNT = 1_000_000_000L

    /** 인코딩된 물음표 — 그 뒤의 검색어가 경로로 남는다 */
    private val ENCODED_QUESTION = Regex("%3[Ff]")
    /** 주소 꼴을 보기 전에 푸는 인코딩 — 콜론 · 점 · 대괄호 */
    private val ENCODED_ADDRESS_CHARACTERS = mapOf(Regex("%3[Aa]") to ":", Regex("%2[Ee]") to ".", Regex("%5[Bb]") to "[", Regex("%5[Dd]") to "]")
    /** 경로 안의 주소 꼴 — IPv4(점으로 이은 수 넷 · 앞뒤가 숫자가 아님) · 완전한 IPv6(16진 1~4자 묶음 여덟) · 겹콜론 꼴(`::` 의 앞이나 뒤에 16진 묶음) · 대괄호 안의 콜론 */
    private val ADDRESS_SHAPES = listOf(
        Regex("(?<![0-9])[0-9]{1,3}(\\.[0-9]{1,3}){3}(?![0-9])"),
        Regex("(?<![0-9A-Fa-f])[0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){7}(?![0-9A-Fa-f])"),
        Regex("(?<![0-9A-Za-z])[0-9A-Fa-f]{1,4}::|::[0-9A-Fa-f]{1,4}(?![0-9A-Za-z])"),
        Regex("\\[[^\\]]*:[^\\]]*]"),
    )

    // 객체 뒤에 더 붙은 글자도 거절한다 — 지표의 해석과 같은 설정
    private val json = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build()

    /** 꼴이 틀림 — 그 자리에서 해석을 멈춘다 */
    private class Shape : RuntimeException(null, null, false, false)

    /** [receivedAt] 은 관측 서버가 이 항목을 받은 시각 — [TrafficEntry.receivedAt] 에 그대로 */
    fun parse(record: MetricStreamRecord, receivedAt: Instant): TrafficEntryParse {
        StreamId.parse(record.id) ?: return TrafficEntryParse.Dropped(TrafficDropReason.INVALID_ID)
        val fields = record.fields
        if (fields.isEmpty()) return TrafficEntryParse.Dropped(TrafficDropReason.TRIMMED)
        val kind = fields["kind"]?.takeIf { it.isNotEmpty() } ?: return TrafficEntryParse.Dropped(TrafficDropReason.MISSING_FIELD)
        if (kind != KIND) return TrafficEntryParse.OtherKind(kind)
        if (STREAM_FIELDS.any { fields[it].isNullOrEmpty() }) return TrafficEntryParse.Dropped(TrafficDropReason.MISSING_FIELD)
        if (fields["v"] != VERSION) return TrafficEntryParse.Dropped(TrafficDropReason.UNSUPPORTED_VERSION)
        val server = fields.getValue("server")
        if (!SERVER_NAME.matches(server)) return TrafficEntryParse.Dropped(TrafficDropReason.INVALID_SERVER)
        val generation = fields.getValue("generation")
        if (!GENERATION.matches(generation)) return TrafficEntryParse.Dropped(TrafficDropReason.INVALID_GENERATION)
        val sequence = fields.getValue("seq").takeIf { SEQUENCE.matches(it) }?.toLong() ?: return TrafficEntryParse.Dropped(TrafficDropReason.INVALID_SEQ)
        val readMillis = fields.getValue("at").takeIf { AT.matches(it) }?.toLong()?.takeIf { it > 0 }
            ?: return TrafficEntryParse.Dropped(TrafficDropReason.INVALID_AT)
        val body = fields.getValue("body")
        if (body.toByteArray(Charsets.UTF_8).size > MAX_BODY_BYTES) return TrafficEntryParse.Dropped(TrafficDropReason.BODY_TOO_LARGE)
        val tree = try { json.readTree(body) } catch (_: JacksonException) { return TrafficEntryParse.Dropped(TrafficDropReason.BODY_NOT_JSON) }
        return try {
            TrafficEntryParse.Accepted(entry(tree, server, generation, sequence, Instant.ofEpochMilli(readMillis), receivedAt))
        } catch (_: Shape) {
            TrafficEntryParse.Dropped(TrafficDropReason.INVALID_BODY)
        }
    }

    private fun entry(tree: JsonNode, server: String, generation: String, sequence: Long, readAt: Instant, receivedAt: Instant): TrafficEntry {
        if (!tree.isObject) throw Shape()
        if (array(tree, "sources").any { !it.isObject }) throw Shape()
        array(tree, "dependencies")
        val missing = array(tree, "missing").map { text(it, Int.MAX_VALUE).takeIf { value -> MISSING.matches(value) } ?: throw Shape() }
        val segments = array(tree, "segments").map(::segment)
        if (segments.distinctBy { listOf(it.bucketStart, it.service, it.caller, it.trafficClass) }.size != segments.size) throw Shape()
        val requestNodes = array(tree, "requests")
        if (requestNodes.size > MAX_REQUESTS) throw Shape()
        val requests = requestNodes.map(::request)
        val kept = requests.filter { isPathAllowed(it.path) }
        return TrafficEntry(
            server = server, generation = generation, sequence = sequence, readAt = readAt, receivedAt = receivedAt,
            segments = segments, requests = kept, requestsDropped = count(tree, "requests_dropped"), missing = missing,
            requestsRejected = (requests.size - kept.size).toLong(),
        )
    }

    private fun segment(node: JsonNode): TrafficSegment {
        if (!node.isObject) throw Shape()
        val bucketStart = count(node, "bucket_start_ms").takeIf { it > 0 && it % CELL_MS == 0L } ?: throw Shape()
        val trafficClass = TrafficClass.of(text(field(node, "class"))) ?: throw Shape()
        val count = count(node, "count").takeIf { it in 1..MAX_CELL_COUNT } ?: throw Shape()
        val statusNode = field(node, "status").takeIf { it.isObject } ?: throw Shape()
        val statuses = STATUS_KEYS.map { count(statusNode, it) }
        if (statuses.sum() != count) throw Shape()
        val buckets = array(node, "buckets").map { counted(it).takeIf { value -> value <= MAX_CELL_COUNT } ?: throw Shape() }
        if (buckets.size != TrafficDistribution.BUCKET_COUNT) throw Shape()
        val maxNode = field(node, "max_ms")
        val maxMs = if (trafficClass == TrafficClass.REQUEST) {
            if (buckets.sum() != count) throw Shape()
            counted(maxNode)
        } else {
            if (buckets.any { it != 0L } || !maxNode.isNull) throw Shape()
            null
        }
        return TrafficSegment(
            Instant.ofEpochMilli(bucketStart), name(node, "service"), name(node, "caller"), trafficClass,
            TrafficTotals(count, TrafficStatusCounts(statuses[0], statuses[1], statuses[2], statuses[3], statuses[4]), buckets, maxMs),
        )
    }

    private fun request(node: JsonNode): TrafficRequest {
        if (!node.isObject) throw Shape()
        val path = text(field(node, "path"), MAX_PATH_LENGTH)
        val pathTruncated = field(node, "path_truncated").takeIf { it.isBoolean }?.booleanValue() ?: throw Shape()
        return TrafficRequest(
            requestId = text(field(node, "request_id")).takeIf { REQUEST_ID.matches(it) } ?: throw Shape(),
            service = name(node, "service"),
            caller = name(node, "caller"),
            method = text(field(node, "method")).takeIf { METHOD.matches(it) } ?: throw Shape(),
            path = path,
            pathTruncated = pathTruncated,
            status = count(node, "status").takeIf { it <= MAX_STATUS }?.toInt() ?: throw Shape(),
            endAt = Instant.ofEpochMilli(count(node, "end_ms").takeIf { it > 0 } ?: throw Shape()),
            durationMs = count(node, "duration_ms"),
            upstreamMs = field(node, "upstream_ms").let { if (it.isNull) null else counted(it) },
            upstreamStatus = field(node, "upstream_status").let { if (it.isNull) null else text(it) },
            slot = field(node, "slot").let { if (it.isNull) null else text(it) },
            bytesSent = count(node, "bytes_sent"),
        )
    }

    /**
     * 요청별 줄의 경로로 받아도 되는가 — 날것 `?` · `%3F` 가 있거나, `%3A` · `%2E` · `%5B` · `%5D` 를 푼 뒤 주소 꼴([ADDRESS_SHAPES])이 들었으면 아니다.
     * 에이전트가 막는 것을 받는 쪽에서 한 번 더 막는다(규약 4.5 끝의 "그 줄만 버리는 경우"). IPv4 꼴은 판 번호(`/release/1.2.3.4`)도 버린다 — 받아들인 한계.
     */
    private fun isPathAllowed(path: String): Boolean {
        if ('?' in path || ENCODED_QUESTION.containsMatchIn(path)) return false
        val decoded = ENCODED_ADDRESS_CHARACTERS.entries.fold(path) { text, (encoded, plain) -> text.replace(encoded, plain) }
        return ADDRESS_SHAPES.none { it.containsMatchIn(decoded) }
    }

    /** 칸이 있어야 한다(JSON null 은 있는 것) — 칸은 생략하지 않는다(규약 4.3) */
    private fun field(node: JsonNode, name: String): JsonNode = node.get(name) ?: throw Shape()

    private fun array(node: JsonNode, name: String): List<JsonNode> = field(node, name).takeIf { it.isArray }?.values()?.toList() ?: throw Shape()

    private fun name(node: JsonNode, field: String): String = text(field(node, field)).takeIf { NAME.matches(it) } ?: throw Shape()

    /** 글자 — 제어 문자(PostgreSQL 이 받지 않는 NUL 포함)와 지나친 길이는 꼴이 틀린 것 */
    private fun text(node: JsonNode, maxLength: Int = MAX_TEXT_LENGTH): String {
        if (!node.isString) throw Shape()
        val value = node.stringValue()
        if (value.length > maxLength || value.any { it < ' ' || it == '\u007f' }) throw Shape()
        return value
    }

    /** 0 이상의 정수 칸 */
    private fun count(node: JsonNode, name: String): Long = counted(field(node, name))

    private fun counted(node: JsonNode): Long {
        if (!node.isIntegralNumber || !node.canConvertToLong() || node.longValue() < 0) throw Shape()
        return node.longValue()
    }
}
