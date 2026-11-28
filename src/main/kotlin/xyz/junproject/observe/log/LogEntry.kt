package xyz.junproject.observe.log

import java.time.Instant

/**
 * 스트림 항목의 ID(`<밀리초>-<순번>`). 밀리초는 Redis 가 그 로그를 받은 시각이고, 이 쌍이 로그 한 줄의 신원이다 —
 * 같은 항목을 두 번 받아도 한 번만 저장되게 하는 기본 키이자, 최신순 정렬과 이어 보기의 기준.
 */
data class StreamId(val millis: Long, val sequence: Int) : Comparable<StreamId> {
    val at: Instant get() = Instant.ofEpochMilli(millis)
    override fun compareTo(other: StreamId) = compareValuesBy(this, other, StreamId::millis, StreamId::sequence)
    override fun toString() = "$millis-$sequence"

    companion object {
        private val pattern = Regex("""^(\d{1,15})-(\d{1,9})$""")
        fun parse(text: String): StreamId? = pattern.matchEntire(text)?.let {
            StreamId(it.groupValues[1].toLong(), it.groupValues[2].toInt())
        }
    }
}

/** 저장되는 로그 한 줄 */
data class LogEntry(
    val id: StreamId,
    val level: String,
    val service: String,
    val requestId: String,
    val message: String,
)

/**
 * 발신 쪽 형식 `<요청ID>:<서비스>:<내용>` 한 줄을 나눈다(네 서비스의 로거가 이 형식으로 보낸다 — 발신 쪽은 바꾸지 않는다).
 * 나눌 수 없는 줄도 버리지 않는다: 서비스 `unknown`, 원문을 내용으로.
 * 길이는 자르지 않는다 — 명세 ② "원문 그대로". 긴 오류의 뒷부분(원인 예외)이 찾고 싶은 부분인 경우가 많다.
 */
object LogLineParser {
    const val UNKNOWN_SERVICE = "unknown"
    /** 서비스 이름의 꼴 — 이 꼴이 아닌 줄은 서비스 [UNKNOWN_SERVICE] 로 남는다(로그 무수신 감시의 설정도 이 꼴의 이름만 받는다) */
    val SERVICE_NAME = Regex("[a-z0-9-]{1,40}")
    private val shape = Regex("""^([A-Za-z0-9_.-]{1,80}):(${SERVICE_NAME.pattern}):(.*)$""", RegexOption.DOT_MATCHES_ALL)
    private val knownLevels = setOf("debug", "info", "warning", "error")

    fun parse(id: StreamId, rawLevel: String?, rawLine: String?): LogEntry {
        val line = clean(rawLine ?: "")
        val match = shape.matchEntire(line)
        val level = (rawLevel ?: "").trim().lowercase().let { if (it in knownLevels) it else if (it == "warn") "warning" else "info" }
        return if (match != null) {
            LogEntry(id, level, match.groupValues[2], match.groupValues[1], match.groupValues[3])
        } else {
            LogEntry(id, level, UNKNOWN_SERVICE, "", line)
        }
    }

    /** PostgreSQL 의 text 는 NUL 문자를 받지 않는다 — 한 글자 때문에 묶음 전체의 저장이 실패하지 않게 지운다 */
    private fun clean(line: String) = if (line.indexOf('\u0000') >= 0) line.replace("\u0000", "") else line
}
