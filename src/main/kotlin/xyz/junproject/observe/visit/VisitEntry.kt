package xyz.junproject.observe.visit

import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.MetricStreamRecord

/** 기기 갈래 — 스트림의 `device` 칸 · 표의 `device` 칸 · 요약의 `devices` 칸에 나가는 글자 그대로 */
enum class VisitDevice(val wireName: String) {
    DESKTOP("desktop"),
    MOBILE("mobile"),
    BOT("bot");

    companion object {
        fun of(wireName: String): VisitDevice? = entries.firstOrNull { it.wireName == wireName }
    }
}

/**
 * 저장하는 방문 한 줄(스트림 항목 하나). 방문 시각은 [id] 의 밀리초(Redis 가 받은 시각).
 * [referrerHost] 가 null 이면 들어온 곳 없음(직접). [ip] 는 주소 원문 — 표에만 남고 로그 · 응답 · 헬스 · 예외 글에 나가지 않는다.
 */
data class Visit(
    val id: StreamId,
    val path: String,
    val screen: String,
    val referrerHost: String?,
    val device: VisitDevice,
    val visitor: String,
    val ip: String,
) {
    /** 주소 · 구분값을 싣지 않는다 — 실수로 로그나 예외 글에 찍혀도 새지 않게(기본 data class 표현에는 모든 칸이 들어간다) */
    override fun toString() = "Visit(id=$id, path=$path, screen=$screen, referrerHost=$referrerHost, device=${device.wireName})"
}

/** 버린 항목의 이유 — 헬스의 `visits.dropped` 에 나가는 글자 그대로 */
enum class VisitDropReason(val wireName: String) {
    INVALID_ID("invalid_id"),
    TRIMMED("trimmed"),
    MISSING_FIELD("missing_field"),
    UNSUPPORTED_VERSION("unsupported_version"),
    CONTROL_CHARACTER("control_character"),
    TOO_LONG("too_long"),
    INVALID_PATH("invalid_path"),
    INVALID_SCREEN("invalid_screen"),
    INVALID_REFERRER("invalid_referrer"),
    INVALID_DEVICE("invalid_device"),
    INVALID_VISITOR("invalid_visitor"),
    INVALID_IP("invalid_ip"),
}

/** 스트림 항목 하나를 해석한 결과 */
sealed interface VisitEntryParse {
    /** 저장할 방문. [queryStripped] 는 경로에 붙어 온 `?` · `#` 뒤를 잘라 냈는지 */
    data class Accepted(val visit: Visit, val queryStripped: Boolean) : VisitEntryParse
    /** 꼴이 틀린 항목 — 이유를 세고 확인한다(다시 받지 않는다) */
    data class Dropped(val reason: VisitDropReason) : VisitEntryParse
}

/**
 * 스트림 항목의 해석(순수 함수). 검사 순서: ID → 칸 없음(TRIMMED) → 필수 칸 → 버전 → 제어 문자(받은 글 그대로) → 길이 →
 * 칸마다의 꼴(경로 → 화면 → 들어온 곳 → 기기 → 구분값 → 주소). 경로는 처음 나오는 `?` · `#` 의 앞까지만 남긴다(검색어를 남기지 않는다) —
 * 길이와 꼴은 잘라 낸 뒤의 경로로 본다. 모르는 칸은 보지도 싣지도 않는다.
 */
object VisitEntryParser {
    const val VERSION = "1"
    const val MAX_PATH_LENGTH = 512
    const val MAX_SCREEN_LENGTH = 32
    const val MAX_REFERRER_LENGTH = 253
    const val MAX_IP_LENGTH = 45

    private val REQUIRED_FIELDS = listOf("v", "path", "screen", "device", "visitor", "ip")
    private val CHECKED_FIELDS = listOf("path", "screen", "referrer_host", "device", "visitor", "ip")
    private val SCREEN = Regex("^[a-z][a-z0-9_-]{0,31}$")
    private val HOST_NAME = Regex("^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$")
    private val VISITOR = Regex("^[0-9a-f]{64}$")
    private val IPV4 = Regex("^(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}$")
    private val IPV6_GROUP = Regex("^[0-9A-Fa-f]{1,4}$")

    fun parse(record: MetricStreamRecord): VisitEntryParse {
        val id = StreamId.parse(record.id) ?: return dropped(VisitDropReason.INVALID_ID)
        val fields = record.fields
        if (fields.isEmpty()) return dropped(VisitDropReason.TRIMMED)
        if (REQUIRED_FIELDS.any { fields[it].isNullOrEmpty() }) return dropped(VisitDropReason.MISSING_FIELD)
        if (fields["v"] != VERSION) return dropped(VisitDropReason.UNSUPPORTED_VERSION)
        if (CHECKED_FIELDS.any { field -> fields[field]?.any { Character.isISOControl(it) } == true }) return dropped(VisitDropReason.CONTROL_CHARACTER)
        val receivedPath = fields.getValue("path")
        val cut = receivedPath.indexOfFirst { it == '?' || it == '#' }
        val path = if (cut < 0) receivedPath else receivedPath.substring(0, cut)
        val screen = fields.getValue("screen")
        val referrerHost = fields["referrer_host"]?.takeIf { it.isNotEmpty() }
        val ip = fields.getValue("ip")
        if (path.length > MAX_PATH_LENGTH || screen.length > MAX_SCREEN_LENGTH || (referrerHost?.length ?: 0) > MAX_REFERRER_LENGTH || ip.length > MAX_IP_LENGTH) {
            return dropped(VisitDropReason.TOO_LONG)
        }
        if (!path.startsWith('/') || path.any(Char::isWhitespace)) return dropped(VisitDropReason.INVALID_PATH)
        if (!SCREEN.matches(screen)) return dropped(VisitDropReason.INVALID_SCREEN)
        if (referrerHost != null && !HOST_NAME.matches(referrerHost)) return dropped(VisitDropReason.INVALID_REFERRER)
        val device = VisitDevice.of(fields.getValue("device")) ?: return dropped(VisitDropReason.INVALID_DEVICE)
        val visitor = fields.getValue("visitor")
        if (!VISITOR.matches(visitor)) return dropped(VisitDropReason.INVALID_VISITOR)
        if (!IPV4.matches(ip) && !isIpv6(ip)) return dropped(VisitDropReason.INVALID_IP)
        return VisitEntryParse.Accepted(Visit(id, path, screen, referrerHost, device, visitor, ip), queryStripped = cut >= 0)
    }

    private fun dropped(reason: VisitDropReason) = VisitEntryParse.Dropped(reason)

    /**
     * IPv6 글자 — 16진 1~4자리 묶음 여덟, `::` 는 한 번까지(0 묶음 이상을 줄인다), 끝 두 묶음 자리에 IPv4 를 쓸 수 있다(`::ffff:203.0.113.77`).
     * 존 ID(`%eth0`) · 대괄호 · 포트는 받지 않는다. 이름 풀이(DNS)를 하는 라이브러리 함수를 쓰지 않으려고 글자로만 본다.
     */
    private fun isIpv6(text: String): Boolean {
        val lastColon = text.lastIndexOf(':')
        if (lastColon < 0) return false
        val tail = text.substring(lastColon + 1)
        val groups = if (tail.contains('.')) {
            if (!IPV4.matches(tail)) return false
            text.substring(0, lastColon + 1) + "0:0"                 // IPv4 는 묶음 둘과 같다
        } else text
        val halves = groups.split("::")
        if (halves.size > 2) return false
        fun count(part: String): Int? = if (part.isEmpty()) 0 else part.split(':').takeIf { list -> list.all { IPV6_GROUP.matches(it) } }?.size
        if (halves.size == 1) return count(groups) == 8
        val left = count(halves[0]) ?: return false
        val right = count(halves[1]) ?: return false
        return left + right <= 7
    }
}
