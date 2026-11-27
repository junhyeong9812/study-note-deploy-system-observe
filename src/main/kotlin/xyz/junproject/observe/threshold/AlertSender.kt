package xyz.junproject.observe.threshold

import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
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
 * 보낼 때마다 새 추적 ID 를 머리글 `X-Request-Id` 로 싣는다(통신 규약 §1.2 — 들어온 요청 없이 시작되는 호출. 16진수 소문자 32자 — nginx 의 `$request_id` 와 같은 꼴).
 * 머리글 `X-Caller-Service: observe` 를 하나 싣는다(통신 규약 §2.2 — 값은 코드의 상수. 설정 · 들어온 요청의 머리글에서 읽지 않는다).
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
                .header("X-Request-Id", UUID.randomUUID().toString().replace("-", ""))      // 보낼 때마다 새 값 — 같은 알림을 다시 보낼 때도
                .header(CALLER_SERVICE_HEADER, CALLER_SERVICE)                               // 자기 규약 이름 — 받는 쪽 nginx 가 접속 기록의 caller 로 적는다
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
        const val CALLER_SERVICE_HEADER = "X-Caller-Service"
        /** 관측 서버의 규약 이름(통신 규약 §2.1) — 코드의 상수 */
        const val CALLER_SERVICE = "observe"
        val RETRYABLE_CLIENT_ERRORS = setOf(401, 403, 429)
    }
}

/** 보내기 한 바퀴의 결과 — 잠금을 얻지 못했으면 [lockAcquired] 가 false 이고 아무것도 보내지 않았다 */
data class DispatchOutcome(val lockAcquired: Boolean, val delivered: Int, val retrying: Int, val rejected: Int)

/**
 * 보낼 표를 읽어 보낸다 — 보내기 잠금을 얻은 자리만(같은 알림을 두 자리가 함께 보내지 않는다). 대상마다 줄의 머리만([AlertQueue.pendingByTarget] —
 * 대상마다 오래된 [batchSize] 건), 한 바퀴에 합쳐 [batchSize] 건까지.
 * Delivered → [AlertQueue.markDelivered], Retry → [AlertQueue.markRetry], Rejected → [AlertQueue.markRejected].
 * 보내는 쪽이 예외를 내도 Retry 로 본다. 보내기 잠금은 한 바퀴 동안 쥔다 — 그 길이는 [roundBudget] 를 넘지 않는다.
 */
class AlertDispatcher(
    private val queue: AlertQueue,
    private val sender: AlertSender,
    private val batchSize: Int = 50,
    /**
     * 한 바퀴의 상한(§16.1 — [sendTimeout] 참고).
     */
    private val roundBudget: Duration = Duration.ofSeconds(30),
    private val clock: () -> Instant = Instant::now,
    /**
     * §16 · §16.1: 한 건의 기한. 한 바퀴는 "지금까지 쓴 시간 + 이 기한"이 [roundBudget] 를 **넘게 되면**(같으면 아직) 새로 보내지 않는다 —
     * 바퀴 전체가 [roundBudget] 를 넘지 않는다. 보낼 알림은 [AlertQueue.pendingByTarget] 의 대상마다 줄의 머리만: 대상의 차례는 마지막 시도가
     * 가장 오래된 대상부터(시도한 적 없는 대상이 맨 앞 — 그끼리는 머리가 오래된 순), 머리가 전달 · 거절이면 같은 바퀴에 그 대상의 다음 건이 머리가 되고,
     * "다시 시도"면 그 대상은 이번 바퀴에 더 보내지 않는다. 표시의 `at` 은 그 건을 보낸 시각([clock]).
     */
    private val sendTimeout: Duration = Duration.ofSeconds(3),
) {
    fun dispatch(now: Instant): DispatchOutcome = queue.whileHoldingDispatchLock {
        val started = clock()
        var delivered = 0
        var retrying = 0
        var rejected = 0
        // 대상의 차례: 시도한 적 없는 대상(그끼리는 머리가 오래된 순) → 마지막 시도가 오래된 대상
        val queues = queue.pendingByTarget(batchSize).filter { it.alerts.isNotEmpty() }.sortedWith(
            compareBy<TargetQueue>({ it.lastAttemptAt != null }, { it.lastAttemptAt }, { it.alerts.first().alert.createdAt }, { it.alerts.first().id }))
        // 대상을 돌아가며 한 건씩(§16.2): 차례대로 대상마다 머리 한 건, 다 돌면 처음 대상으로 돌아가 다음 건. 같은 대상 안의 순서는 그대로,
        // "다시 시도"가 난 대상은 그 바퀴에서 빠진다 — 한 대상에 알림이 많이 밀려도 다른 대상이 그 뒤에서 기다리지 않는다
        val remaining = queues.map { ArrayDeque(it.alerts) }.toMutableList()
        sending@ while (remaining.isNotEmpty()) {
            val iterator = remaining.iterator()
            while (iterator.hasNext()) {
                val line = iterator.next()
                if (delivered + retrying + rejected >= batchSize) break@sending
                // 한 건의 기한을 더해 바퀴의 상한을 넘게 되면 새로 보내지 않는다 — 바퀴 전체가 상한을 넘지 않게
                if (Duration.between(started, clock()).plus(sendTimeout) > roundBudget) break@sending
                val queued = line.removeFirst()
                val outcome = runCatching { sender.send(queued.alert) }.getOrElse { SendOutcome.Retry(it.javaClass.simpleName) }
                val at = clock()                                       // 표시의 시각 = 그 건을 보낸 시각(대상의 마지막 시도)
                when (outcome) {
                    SendOutcome.Delivered -> { queue.markDelivered(queued.id, at); delivered++ }
                    is SendOutcome.Retry -> { queue.markRetry(queued.id, at, outcome.reason); retrying++; line.clear() }
                    is SendOutcome.Rejected -> { queue.markRejected(queued.id, at, outcome.reason); rejected++ }
                }
                if (line.isEmpty()) iterator.remove()
            }
        }
        DispatchOutcome(true, delivered, retrying, rejected)
    } ?: DispatchOutcome(false, 0, 0, 0)
}
