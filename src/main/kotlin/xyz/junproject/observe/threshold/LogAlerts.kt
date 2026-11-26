package xyz.junproject.observe.threshold

import java.time.Duration
import java.time.Instant

/** 로그 규칙(⑨) — 서비스별 무수신 · 전체 스트림 한 줄의 무수신. kind 는 둘 다 `log_silent` */
enum class LogRule(override val wireName: String, override val kind: String) : AlertRule {
    SILENT("log_silent", "log_silent"),
    SILENT_ALL("log_silent_all", "log_silent"),
}

/**
 * 뼈대(⑪-6a): 로그 글자 규칙 — 설정한 (서비스 · 글자)가 든 로그 줄. kind `log_pattern`.
 * 구현에서 [AlertRule.entries] 에 더한다(보내기가 모르는 규칙으로 건너뛰지 않게 — 섞인 판에서 옛 판은 모르는 줄로 남긴다).
 */
enum class LogPatternRule(override val wireName: String, override val kind: String) : AlertRule {
    PATTERN("log_pattern", "log_pattern"),
}

/**
 * 로그 알림을 만든다 — 보낼 표에 자원 알림과 같은 꼴([ResourceAlert])로 든다. 넘음 warning · 해소 info, 고유 키 = `<key>@<판정 시각 epoch 밀리초>`.
 * 글에 드는 것은 서비스 · 서버 이름 · 마지막으로 받은 시각 · 기준뿐이다 — 로그의 글은 인자로 받지 않는다(글 · 주소가 실릴 수 없는 꼴).
 * 서비스 · 서버 이름은 발신 쪽 · 설정의 글자라 숨은 문자를 '?' 로 바꿔 싣는다([visibleText]). key 는 120자 이하([boundedBaseKey]).
 * key 는 서비스가 정한다(서버는 들지 않는다). 서로 다른 서비스 · 전체 한 줄의 넘음 · 해소는 어떤 이름에서도 key 가 겹치지 않는다 —
 * 겹치면 보낼 표가 같은 고유 키의 뒤 알림을 조용히 버린다. title · detail · key 의 자르기는 글자(코드포인트)의 경계에서 한다.
 */
object LogAlerts {
    /**
     * 서비스 하나의 무수신 — key `log:<서비스>:silent`(해소는 뒤에 `:resolved`), [server] 는 그 서비스가 사는 서버(설정),
     * 대상 이름 = 서비스(다듬은 글자), [lastReceivedAt] 은 마지막 로그를 받은 시각(없으면 null).
     * 다듬어 글자가 바뀌었거나 `~` 가 든 이름은 key 가 `log:<다듬은 이름>~<원래 이름의 해시>:silent` — 숨은 문자만 다른 이름 ·
     * 그 꼴을 글자 그대로 흉내 낸 이름이 한 key 가 되지 않게
     */
    fun silent(service: String, server: String, lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert {
        val shownService = visibleText(service)
        val shownServer = visibleText(server)
        val title = if (resolved) "$shownService 로그 다시 들어옴" else "$shownService 로그 끊김 — ${minutes(threshold)} 넘게 안 들어옴"
        val keyName = if (shownService == service && HASH_MARK !in service) service else "$shownService$HASH_MARK${shortHash(service)}"
        return alert(LogRule.SILENT, "log:$keyName:silent", shownServer, shownService, resolved, now, title,
            "서비스 $shownService · 서버 $shownServer · ${received(lastReceivedAt)} · 기준 ${minutes(threshold)}")
    }

    /** 전체 스트림 한 줄의 무수신 — key `log-all:silent`(서비스의 자리 `log:` 와 다른 글자라 어떤 서비스 이름과도 겹치지 않는다), 서버 `*`, 대상 이름 없음 */
    fun silentAll(lastReceivedAt: Instant?, threshold: Duration, now: Instant, resolved: Boolean): ResourceAlert {
        val title = if (resolved) "전체 로그 다시 들어옴" else "전체 로그 끊김 — ${minutes(threshold)} 넘게 안 들어옴"
        return alert(LogRule.SILENT_ALL, "log-all:silent", ALL_SERVERS, null, resolved, now, title,
            "전체 스트림 · ${received(lastReceivedAt)} · 기준 ${minutes(threshold)}")
    }

    /** 관측 서버가 받는 요청 ID 의 꼴 — 그 밖이면 사건에 싣지 않는다 */
    val REQUEST_ID = Regex("^[A-Za-z0-9_.-]{1,80}$")

    /**
     * 뼈대(⑪-6a): 글자 규칙 하나의 사건 — 고유 키 `log_pattern:<이름>@<now 밀리초>`, key `log_pattern:<이름>`, kind `log_pattern`, 세기 = 규칙의 세기, 서버 `*`,
     * 규칙 [LogPatternRule.PATTERN], 대상 이름 = 규칙 이름, 만든 시각 [now].
     * title `로그 글자 규칙 <이름> — <서비스>`(200자 안), detail `<count>줄 맞음 · 첫 줄 요청 ID <ID> · 시각 <ISO>` — 요청 ID 가 [REQUEST_ID] 밖이거나 없으면 `없음`,
     * 시각이 없으면 `모름`. 로그의 내용도 규칙의 글자도 싣지 않는다. 본문은 구현 워커가 채운다.
     */
    fun pattern(pattern: xyz.junproject.observe.logalert.LogPattern, count: Long, firstRequestId: String?, firstAt: Instant?, now: Instant): ResourceAlert =
        TODO("뼈대: 구현 워커가 채운다")

    private const val ALL_SERVERS = "*"
    private const val HASH_MARK = "~"

    private fun alert(rule: LogRule, fullKey: String, server: String, targetName: String?, resolved: Boolean, now: Instant,
                      title: String, detail: String): ResourceAlert {
        val base = boundedBaseKey(fullKey, ::withinLength)
        val key = if (resolved) "$base$RESOLVED_SUFFIX" else base
        return ResourceAlert("$key@${now.toEpochMilli()}", key, rule.kind, if (resolved) AlertSeverity.INFO else AlertSeverity.WARNING,
            withinLength(title, ResourceAlert.MAX_TITLE), withinLength(detail, ResourceAlert.MAX_DETAIL), server, rule, targetName, now)
    }

    /**
     * [limit](UTF-16 단위 — 알림 서버가 그렇게 센다) 안에서 글자의 경계로 자른다 — 끝에 상위 서로게이트가 걸리면 한 단위를 덜 자른다
     * (짝 없는 서로게이트가 남지 않게)
     */
    private fun withinLength(text: String, limit: Int): String {
        if (text.length <= limit) return text
        val end = if (Character.isHighSurrogate(text[limit - 1])) limit - 1 else limit
        return text.substring(0, end)
    }

    private fun received(lastReceivedAt: Instant?) = if (lastReceivedAt == null) "받은 기록 없음" else "마지막으로 받은 시각 $lastReceivedAt"

    private fun minutes(duration: Duration) = if (duration.toSeconds() % 60 == 0L) "${duration.toMinutes()}분" else "${duration.toSeconds()}초"
}
