package xyz.junproject.observe.threshold

import java.time.Duration
import java.time.Instant

/** 한 번 보낸 결과 */
sealed interface SendOutcome {
    /** 알림 서버가 받았다(2xx) */
    data object Delivered : SendOutcome
    /** 닿지 못했거나(연결 실패 · 기한 초과) 지금은 못 받는다(5xx · 429 · 401 · 403 — 설정을 고치면 받을 수 있다) — 다음 바퀴에 다시 */
    data class Retry(val reason: String) : SendOutcome
    /** 본문을 거절했다(그 밖의 4xx — 같은 본문을 다시 보내도 같은 답) — 다시 보내지 않는다 */
    data class Rejected(val reason: String) : SendOutcome
}

/** 알림 서버로 보내는 면 — 예외를 내지 않는다(실패는 [SendOutcome] 으로) */
interface AlertSender {
    fun send(alert: ResourceAlert): SendOutcome
}

/**
 * 알림 서버 `POST /events` — 머리글 `X-Alert-Secret`, `Content-Type: application/json`, 본문 [ResourceAlert.eventBody].
 * [eventsUrl] 은 `/events` 까지 적은 주소(설정 `observe.alert.events-url` · `ALERT_EVENTS_URL` — ci-cd 와 같은 이름),
 * [secret] 은 `observe.alert.secret` · `ALERT_SECRET`, 기한은 연결과 응답 전체에 `observe.alert.timeout-ms` · `ALERT_TIMEOUT_MS`(기본 3초).
 * 주소가 비어 있으면 보내지 않고 `Retry("not_configured")` — 알림이 남아 헬스의 못 보낸 수로 보인다.
 * 사유 글자: 응답이면 `"http <코드>"`, 예외면 예외 종류 이름(문구 · 주소는 넣지 않는다).
 */
class HttpAlertSender(
    private val eventsUrl: String,
    private val secret: String,
    private val timeout: Duration = Duration.ofSeconds(3),
) : AlertSender {
    override fun send(alert: ResourceAlert): SendOutcome = TODO("not implemented")
}

/** 보내기 한 바퀴의 결과 — 잠금을 얻지 못했으면 [lockAcquired] 가 false 이고 아무것도 보내지 않았다 */
data class DispatchOutcome(val lockAcquired: Boolean, val delivered: Int, val retrying: Int, val rejected: Int)

/**
 * 보낼 표를 읽어 보낸다 — 보내기 잠금을 얻은 자리만(같은 알림을 두 자리가 함께 보내지 않는다). 오래된 것부터 [batchSize] 건.
 * Delivered → [AlertQueue.markDelivered], Retry → [AlertQueue.markRetry], Rejected → [AlertQueue.markRejected].
 * 보내는 쪽이 예외를 내도 Retry 로 보고 나머지를 계속 보낸다.
 */
class AlertDispatcher(
    private val queue: AlertQueue,
    private val sender: AlertSender,
    private val batchSize: Int = 50,
) {
    fun dispatch(now: Instant): DispatchOutcome = TODO("not implemented")
}
