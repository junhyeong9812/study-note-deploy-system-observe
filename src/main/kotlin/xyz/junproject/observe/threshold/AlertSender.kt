package xyz.junproject.observe.threshold

import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

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
    private val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(timeout).build()
    private val json = JsonMapper.builder().build()

    override fun send(alert: ResourceAlert): SendOutcome {
        if (eventsUrl.isBlank()) return SendOutcome.Retry(NOT_CONFIGURED)
        return try {
            val request = HttpRequest.newBuilder(URI(eventsUrl.trim())).timeout(timeout)
                .header("Content-Type", "application/json").header("X-Alert-Secret", secret)
                .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(alert.eventBody()))).build()
            // 연결 · 응답 전체를 기한 하나로 — 응답 머리 뒤에 본문이 늦어도 기한에서 끊는다
            val pending = client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            val status = try {
                pending.get(timeout.toMillis(), TimeUnit.MILLISECONDS).statusCode()
            } catch (error: TimeoutException) {
                pending.cancel(true)
                throw error
            }
            outcome(status)
        } catch (error: ExecutionException) {
            SendOutcome.Retry((error.cause ?: error).javaClass.simpleName)             // 주소 · 문구는 싣지 않는다
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            SendOutcome.Retry(error.javaClass.simpleName)
        } catch (error: Exception) {
            SendOutcome.Retry(error.javaClass.simpleName)
        }
    }

    private fun outcome(status: Int): SendOutcome = when {
        status in 200..299 -> SendOutcome.Delivered
        status >= 500 || status in RETRYABLE_CLIENT_ERRORS -> SendOutcome.Retry("http $status")       // 401 · 403 은 비밀값을 고치면 풀린다
        status in 400..499 -> SendOutcome.Rejected("http $status")                                    // 같은 본문을 다시 보내도 같은 답
        else -> SendOutcome.Retry("http $status")                                                     // 넘겨주기(3xx) 등 — 주소 설정의 문제
    }

    private companion object {
        const val NOT_CONFIGURED = "not_configured"
        val RETRYABLE_CLIENT_ERRORS = setOf(401, 403, 429)
    }
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
    /**
     * 리뷰 루프 1(§15.1 — 구현 전): 한 바퀴의 보내기가 [clock] 으로 잰 합계 [roundBudget] 를 **넘기면**(같으면 아직) 다음 건을 보내지 않고 멈춘다.
     * "다시 시도"가 나온 알림과 같은 대상(서버 · 규칙 · 대상 이름)의 뒤 알림은 그 바퀴에 보내지도 표시하지도 않는다(같은 대상 안의 순서 유지) —
     * 다른 대상은 계속 보낸다. "거절"은 건너뛰고 계속한다(같은 대상의 뒤 알림도 보낸다).
     */
    private val roundBudget: Duration = Duration.ofSeconds(30),
    private val clock: () -> Instant = Instant::now,
) {
    fun dispatch(now: Instant): DispatchOutcome = queue.whileHoldingDispatchLock {
        var delivered = 0
        var retrying = 0
        var rejected = 0
        for (queued in queue.pendingAlerts(batchSize)) {
            when (val outcome = runCatching { sender.send(queued.alert) }.getOrElse { SendOutcome.Retry(it.javaClass.simpleName) }) {
                SendOutcome.Delivered -> { queue.markDelivered(queued.id, now); delivered++ }
                is SendOutcome.Retry -> { queue.markRetry(queued.id, now, outcome.reason); retrying++ }
                is SendOutcome.Rejected -> { queue.markRejected(queued.id, now, outcome.reason); rejected++ }
            }
        }
        DispatchOutcome(true, delivered, retrying, rejected)
    } ?: DispatchOutcome(false, 0, 0, 0)
}
