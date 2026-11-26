package xyz.junproject.observe.threshold

import java.time.Duration
import java.time.Instant

/** 로그 규칙(⑨) — 서비스별 무수신 · 전체 스트림 한 줄의 무수신. kind 는 둘 다 `log_silent` */
enum class LogRule(override val wireName: String, override val kind: String) : AlertRule {
    SILENT("log_silent", "log_silent"),
    SILENT_ALL("log_silent_all", "log_silent"),
}

/**
 * 로그 알림을 만든다 — 보낼 표에 자원 알림과 같은 꼴([ResourceAlert])로 든다. 넘음 warning · 해소 info, 고유 키 = `<key>@<판정 시각 epoch 밀리초>`.
 * 글에 드는 것은 서비스 · 서버 이름 · 마지막으로 받은 시각 · 기준뿐이다 — 로그의 글은 인자로 받지 않는다(글 · 주소가 실릴 수 없는 꼴).
 * 서비스 · 서버 이름은 발신 쪽 · 설정의 글자라 숨은 문자를 '?' 로 바꿔 싣는다([visibleText]). key 는 120자 이하([boundedBaseKey]).
 */
object LogAlerts {
    /**
     * 서비스 하나의 무수신 — key `log:<서비스>:silent`(해소는 뒤에 `:resolved`), [server] 는 그 서비스가 사는 서버(설정),
     * 대상 이름 = 서비스, [lastReceivedAt] 은 마지막 로그를 받은 시각(없으면 null)
     */
    fun silent(service: String, server: String, lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert {
        val shownService = visibleText(service)
        val shownServer = visibleText(server)
        val title = if (resolved) "$shownService 로그 다시 들어옴" else "$shownService 로그 끊김 — ${minutes(threshold)} 넘게 안 들어옴"
        return alert(LogRule.SILENT, "log:$shownService:silent", shownServer, shownService, resolved, now, title,
            "서비스 $shownService · 서버 $shownServer · ${received(lastReceivedAt)} · 기준 ${minutes(threshold)}")
    }

    /** 전체 스트림 한 줄의 무수신 — key `log:*:silent`, 서버 `*`, 대상 이름 없음 */
    fun silentAll(lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert {
        val title = if (resolved) "전체 로그 다시 들어옴" else "전체 로그 끊김 — ${minutes(threshold)} 넘게 안 들어옴"
        return alert(LogRule.SILENT_ALL, "log:*:silent", ALL_SERVERS, null, resolved, now, title,
            "전체 스트림 · ${received(lastReceivedAt)} · 기준 ${minutes(threshold)}")
    }

    private const val ALL_SERVERS = "*"

    private fun alert(rule: LogRule, fullKey: String, server: String, targetName: String?, resolved: Boolean, now: Instant,
                      title: String, detail: String): ResourceAlert {
        val base = boundedBaseKey(fullKey)
        val key = if (resolved) "$base$RESOLVED_SUFFIX" else base
        return ResourceAlert("$key@${now.toEpochMilli()}", key, rule.kind, if (resolved) AlertSeverity.INFO else AlertSeverity.WARNING,
            title.take(ResourceAlert.MAX_TITLE), detail.take(ResourceAlert.MAX_DETAIL), server, rule, targetName, now)
    }

    private fun received(lastReceivedAt: Instant?) = if (lastReceivedAt == null) "받은 기록 없음" else "마지막으로 받은 시각 $lastReceivedAt"

    private fun minutes(duration: Duration) = if (duration.toSeconds() % 60 == 0L) "${duration.toMinutes()}분" else "${duration.toSeconds()}초"
}
