package xyz.junproject.observe.metric

import tools.jackson.core.JacksonException
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
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
     * GPU 온도는 −50~150, 초당 바이트 1e13 이하, 부하 1e6 이하, 컨테이너 CPU 1e6 이하(코어 하나 = 100 이라 100 은 넘을 수 있다),
     * 바이트 수·재시작 횟수·`interval_ms` 는 정수(바이트 수는 정수 칸의 범위가 상한)
     */
    INVALID_VALUE("invalid_value"),
    /**
     * 측정 시각이 수신 시각보다 6일 넘게 뒤다(§15.1 — 원본 보관 7일과 하루의 여유). 원본이 이미 지워진 구간의 5분 평균이
     * 그 한 건으로 덮어써지지 않게 버린다. 정확히 6일은 받는다
     */
    TOO_OLD("too_old"),
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
    /** 측정 시각이 수신 시각보다 이만큼 넘게 뒤면 버린다(§15.1 — 원본 보관 7일과 하루의 여유) */
    val MAX_LATENESS: Duration = Duration.ofDays(6)
    /** agent 의 `SERVER_NAME`(예: `ci-agent-158`) — 조회의 `server` 도 같은 꼴 */
    val SERVER_NAME = Regex("^[a-z0-9][a-z0-9._-]{0,62}$")

    private val AT = Regex("^[0-9]{1,15}$")
    private val CONTAINER_NAME = Regex("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
    private val CONTAINER_ID = Regex("^[0-9a-f]{12,64}$")
    private const val MAX_TEXT_LENGTH = 128
    private const val MAX_GPU_INDEX = 999
    private const val MAX_PERCENT = 100.0
    private const val MIN_TEMPERATURE = -50.0
    private const val MAX_TEMPERATURE = 150.0
    /** 값의 상한(§16 · §16.1) — 유한하다는 것만으로는 받지 않는다. 바이트 수는 정수 칸의 범위가 곧 상한이라 따로 두지 않는다 */
    private const val MAX_BYTES_PER_SECOND = 1e13
    private const val MAX_LOAD = 1e6
    private const val MAX_CONTAINER_CPU = 1e6
    private val REQUIRED_FIELDS = listOf("v", "server", "at", "body")

    // NaN·Infinity 는 기본으로 거절된다(JSON 밖의 낱말). 객체 뒤에 더 붙은 글자도 거절한다 — `{} {}` 를 앞 객체만 읽고 받지 않게
    private val json = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build()

    /** 꼴이 틀림 — 그 자리에서 해석을 멈춘다 */
    private class Shape : RuntimeException(null, null, false, false)

    /** [receivedAt] 은 관측 서버가 이 항목을 받은 시각 — 시계 앞섬 판정과 저장의 수신 시각 */
    fun parse(record: MetricStreamRecord, receivedAt: Instant): MetricEntryParse {
        val id = StreamId.parse(record.id) ?: return MetricEntryParse.Dropped(MetricDropReason.INVALID_ID)
        val fields = record.fields
        if (fields.isEmpty()) return MetricEntryParse.Dropped(MetricDropReason.TRIMMED)
        val kind = fields["kind"]?.takeIf { it.isNotEmpty() } ?: return MetricEntryParse.Dropped(MetricDropReason.MISSING_FIELD)
        if (kind != KIND) return MetricEntryParse.OtherKind(kind)
        if (REQUIRED_FIELDS.any { fields[it].isNullOrEmpty() }) return MetricEntryParse.Dropped(MetricDropReason.MISSING_FIELD)
        if (fields["v"] != VERSION) return MetricEntryParse.Dropped(MetricDropReason.UNSUPPORTED_VERSION)
        val server = fields.getValue("server")
        if (!SERVER_NAME.matches(server)) return MetricEntryParse.Dropped(MetricDropReason.INVALID_SERVER)
        val measuredMillis = fields.getValue("at").takeIf { AT.matches(it) }?.toLong()?.takeIf { it > 0 }
            ?: return MetricEntryParse.Dropped(MetricDropReason.INVALID_AT)
        val measuredAt = Instant.ofEpochMilli(measuredMillis)
        if (measuredAt.isAfter(receivedAt.plus(MAX_CLOCK_AHEAD))) return MetricEntryParse.Dropped(MetricDropReason.CLOCK_AHEAD)
        if (measuredAt.isBefore(receivedAt.minus(MAX_LATENESS))) return MetricEntryParse.Dropped(MetricDropReason.TOO_OLD)
        val body = fields.getValue("body")
        if (body.toByteArray(Charsets.UTF_8).size > MAX_BODY_BYTES) return MetricEntryParse.Dropped(MetricDropReason.BODY_TOO_LARGE)
        val tree = try { json.readTree(body) } catch (_: JacksonException) { return MetricEntryParse.Dropped(MetricDropReason.BODY_NOT_JSON) }
        val reader = BodyReader()
        val sample = try {
            reader.sample(tree, id, server, measuredAt, receivedAt)
        } catch (_: Shape) {
            return MetricEntryParse.Dropped(MetricDropReason.INVALID_BODY)
        }
        // 꼴을 끝까지 본 뒤에 값 — 꼴과 값이 둘 다 틀리면 꼴(invalid_body)이 이유다
        if (reader.valueInvalid) return MetricEntryParse.Dropped(MetricDropReason.INVALID_VALUE)
        return MetricEntryParse.Accepted(sample)
    }

    /** 본문 한 번의 해석 — 꼴이 틀리면 [Shape] 를 던지고, 값이 틀리면 [valueInvalid] 만 표시하고 계속 본다 */
    private class BodyReader {
        var valueInvalid = false

        fun sample(tree: JsonNode, id: StreamId, server: String, measuredAt: Instant, receivedAt: Instant): ResourceSample {
            if (!tree.isObject) throw Shape()
            return ResourceSample(
                id = id, server = server, measuredAt = measuredAt, receivedAt = receivedAt,
                intervalMs = count(tree, "interval_ms"),
                host = present(tree, "host")?.let(::host),
                containers = list(tree, "containers").map(::container),
                gpus = list(tree, "gpus").map(::gpu),
                missing = list(tree, "missing").map { if (it.isString) text(it.stringValue()) else throw Shape() },
            )
        }

        private fun host(node: JsonNode): HostValues {
            if (!node.isObject) throw Shape()
            return HostValues(
                cpuPct = decimal(node, "cpu_pct", maximum = MAX_PERCENT),
                load1 = decimal(node, "load1", maximum = MAX_LOAD),
                memTotal = count(node, "mem_total"),
                memUsed = count(node, "mem_used"),
                diskTotal = count(node, "disk_total"),
                diskUsed = count(node, "disk_used"),
                netRxBps = decimal(node, "net_rx_bps", maximum = MAX_BYTES_PER_SECOND),
                netTxBps = decimal(node, "net_tx_bps", maximum = MAX_BYTES_PER_SECOND),
                ioReadBps = decimal(node, "io_read_bps", maximum = MAX_BYTES_PER_SECOND),
                ioWriteBps = decimal(node, "io_write_bps", maximum = MAX_BYTES_PER_SECOND),
            )
        }

        private fun container(node: JsonNode): ContainerValues {
            if (!node.isObject) throw Shape()
            val name = requiredText(node, "name").takeIf { CONTAINER_NAME.matches(it) } ?: throw Shape()
            val id = requiredText(node, "id").takeIf { CONTAINER_ID.matches(it) } ?: throw Shape()
            return ContainerValues(
                name = name, id = id, project = requiredText(node, "project"),
                cpuPct = decimal(node, "cpu_pct", maximum = MAX_CONTAINER_CPU),   // 코어 하나 = 100 — 100 을 넘을 수 있다
                memUsed = count(node, "mem_used"),
                restarts = count(node, "restarts"),
                state = present(node, "state")?.let { if (it.isString) text(it.stringValue()) else throw Shape() },
            )
        }

        private fun gpu(node: JsonNode): GpuValues {
            if (!node.isObject) throw Shape()
            val index = present(node, "index")?.takeIf { it.isIntegralNumber && it.canConvertToInt() }?.intValue()?.takeIf { it in 0..MAX_GPU_INDEX }
                ?: throw Shape()
            return GpuValues(
                index = index,
                utilPct = decimal(node, "util_pct", maximum = MAX_PERCENT),
                memUsed = count(node, "mem_used"),
                memTotal = count(node, "mem_total"),
                tempC = decimal(node, "temp_c", minimum = MIN_TEMPERATURE, maximum = MAX_TEMPERATURE),
            )
        }

        /** 칸이 없거나 JSON null 이면 null */
        private fun present(node: JsonNode, field: String): JsonNode? = node.get(field)?.takeUnless { it.isNull || it.isMissingNode }

        private fun list(node: JsonNode, field: String): List<JsonNode> {
            val value = present(node, field) ?: return emptyList()
            if (!value.isArray) throw Shape()
            return value.values().toList()
        }

        private fun requiredText(node: JsonNode, field: String): String {
            val value = present(node, field) ?: throw Shape()
            if (!value.isString) throw Shape()
            return text(value.stringValue())
        }

        /** 저장하는 글자 — 제어 문자(PostgreSQL 이 받지 않는 NUL 포함)와 지나친 길이는 꼴이 틀린 것 */
        private fun text(value: String): String {
            if (value.length > MAX_TEXT_LENGTH || value.any { it < ' ' || it == '\u007f' }) throw Shape()
            return value
        }

        /** 0 이상의 정수(바이트 수 · 횟수 · 밀리초) */
        private fun count(node: JsonNode, field: String): Long? {
            val value = present(node, field) ?: return null
            if (!value.isIntegralNumber || !value.canConvertToLong() || value.longValue() < 0) { valueInvalid = true; return null }
            return value.longValue()
        }

        /** 범위 안의 수(정수로 와도 받는다) */
        private fun decimal(node: JsonNode, field: String, minimum: Double = 0.0, maximum: Double = Double.MAX_VALUE): Double? {
            val value = present(node, field) ?: return null
            val number = value.takeIf { it.isNumber }?.doubleValue()
            if (number == null || !number.isFinite() || number < minimum || number > maximum) { valueInvalid = true; return null }
            return number
        }
    }
}

/** 컨테이너의 계열 이름 — 자리 이름(`-blue` · `-green`)을 뗀 이름(§11.5) */
object ContainerName {
    /** 끝의 `-blue` · `-green` 하나만 뗀다. 떼면 빈 이름이 되거나 그 꼴로 끝나지 않으면 그대로 */
    private val SLOTS = listOf("-blue", "-green")

    fun seriesName(actualName: String): String {
        val slot = SLOTS.firstOrNull { actualName.endsWith(it) } ?: return actualName
        return actualName.dropLast(slot.length).ifEmpty { actualName }
    }
}
