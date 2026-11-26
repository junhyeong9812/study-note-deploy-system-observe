package xyz.junproject.observe.threshold

import java.time.Duration
import java.time.Instant

/** 로그 규칙(⑨) — 서비스별 무수신 · 전체 스트림 한 줄의 무수신. 뼈대(02b) */
enum class LogRule(override val wireName: String, override val kind: String) : AlertRule {
    SILENT("log_silent", "log_silent"),
    SILENT_ALL("log_silent_all", "log_silent"),
}

/** 로그 알림을 만든다 — 보낼 표에 자원 알림과 같은 꼴로 든다. 뼈대(02b): 아직 없다 */
object LogAlerts {
    /** 서비스 하나의 무수신 — [server] 는 그 서비스가 사는 서버(설정), [lastReceivedAt] 은 마지막 로그를 받은 시각(없으면 null) */
    fun silent(service: String, server: String, lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert =
        TODO("02b: 서비스별 무수신 알림")

    /** 전체 스트림 한 줄의 무수신 */
    fun silentAll(lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert =
        TODO("02b: 전체 스트림 무수신 알림")
}
