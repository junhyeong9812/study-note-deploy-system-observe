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
)

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

/** 스트림 항목의 해석(순수 함수) */
object VisitEntryParser {
    const val VERSION = "1"
    const val MAX_PATH_LENGTH = 512
    const val MAX_SCREEN_LENGTH = 32
    const val MAX_REFERRER_LENGTH = 253
    const val MAX_IP_LENGTH = 45

    fun parse(record: MetricStreamRecord): VisitEntryParse = TODO("visit-tracking V2 구현")
}
