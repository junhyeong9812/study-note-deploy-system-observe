package xyz.junproject.observe.api

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogQuery
import xyz.junproject.observe.log.LogReader
import xyz.junproject.observe.log.StreamId
import java.security.MessageDigest
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

/** HTTP 경계 — 조회는 서버 간 비밀값, 헬스는 누구나. 조건 검증은 여기서 끝내고 저장소에는 정리된 값만 넘긴다. */
@RestController
class LogController(
    private val reader: LogReader,
    private val health: CollectorHealth,
    @Value("\${observe.secret:}") private val secret: String,
) {
    private val log = LoggerFactory.getLogger("observe")

    private fun authorized(request: HttpServletRequest): Boolean {
        val provided = request.getHeader("X-Observe-Secret") ?: return false
        // 설정이 비어 있으면 항상 거절(fail-closed), 비교는 상수 시간
        return secret.isNotBlank() && MessageDigest.isEqual(provided.toByteArray(), secret.toByteArray())
    }

    private fun unauthorized() = ResponseEntity.status(401).body(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")))
    private fun invalid(detail: String) =
        ResponseEntity.status(422).body(mapOf("success" to false, "error" to mapOf("code" to "invalid_request", "detail" to detail)))

    private class Invalid(val detail: String) : RuntimeException(detail)

    /**
     * 조회 중 DB 오류를 정해진 꼴로 — 시간 초과(statement_timeout, SQLSTATE 57014)는 504 `query_timeout`, 그 밖은 503 `query_failed`.
     * 예외 문구(SQL·접속 주소)는 응답에 넣지 않고 서버 로그에만 남긴다.
     */
    private fun answering(what: String, block: () -> ResponseEntity<out Any>): ResponseEntity<out Any> = try {
        block()
    } catch (error: DataAccessException) {
        val timedOut = error is QueryTimeoutException ||
            generateSequence<Throwable>(error) { it.cause }.any { it is SQLException && it.sqlState == QUERY_CANCELED }
        log.error("observe:observe:{} failed{}: {}: {}", what, if (timedOut) " (timeout)" else "", error.javaClass.simpleName, error.message?.take(500))
        val (status, code) = if (timedOut) 504 to "query_timeout" else 503 to "query_failed"
        ResponseEntity.status(status).body(mapOf("success" to false, "error" to mapOf("code" to code)))
    }

    private fun instant(name: String, text: String?): Instant? =
        try { text?.takeIf { it.isNotBlank() }?.let(Instant::parse) } catch (_: DateTimeParseException) { throw Invalid("$name 은 ISO 시각이어야 한다(예: 2026-11-21T00:00:00Z)") }

    /** 기간: 둘 다 없으면 최근 [defaultSpan]. from 만 있으면 지금까지, to 만 있으면 그 앞 [defaultSpan]. */
    private fun range(from: String?, to: String?, defaultSpan: Duration, maximum: Duration, now: Instant): Pair<Instant, Instant> {
        val end = instant("to", to) ?: now
        val start = instant("from", from) ?: end.minus(defaultSpan)
        if (start.isAfter(end)) throw Invalid("from 이 to 보다 늦다")
        if (Duration.between(start, end) > maximum) throw Invalid("기간은 최대 ${maximum.toDays()}일")
        return start to end
    }

    /**
     * 로그 조회 — 최신순. 기간을 주지 않으면 최근 1시간.
     * 이어 보기: 다음 쪽은 응답의 `next_before` 를 `before` 로, **응답의 `from`·`to` 를 그대로 함께** 넘긴다(`next_before` 가 비면 끝).
     * 기간 없이 `before` 만 넘기면 422 — 쪽마다 "지금부터 1시간"을 다시 세면 창의 오래된 끝이 밀려 그 사이 로그가 조용히 빠진다.
     */
    @GetMapping("/logs")
    fun logs(
        request: HttpServletRequest,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
        @RequestParam(required = false) service: String?,
        @RequestParam(required = false) level: String?,
        @RequestParam(name = "request_id", required = false) requestId: String?,
        @RequestParam(required = false) q: String?,
        @RequestParam(required = false) before: String?,
        @RequestParam(required = false) limit: String?,
    ): ResponseEntity<out Any> = logs(request, from, to, service, level, requestId, q, before, limit, Instant.now())

    internal fun logs(request: HttpServletRequest, from: String?, to: String?, service: String?, level: String?, requestId: String?,
                      q: String?, before: String?, limit: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val query = try {
            if (!before.isNullOrBlank() && from.isNullOrBlank() && to.isNullOrBlank()) throw Invalid("이어 보려면 응답의 from·to 를 함께 넘긴다")
            val (start, end) = range(from, to, Duration.ofHours(1), MAX_RANGE, now)
            LogQuery(
                from = start, to = end,
                service = service?.takeIf { it.isNotBlank() }?.also { if (!it.matches(SERVICE)) throw Invalid("service 형식이 다르다") },
                level = level?.takeIf { it.isNotBlank() }?.also { if (it !in LEVELS) throw Invalid("level 은 ${LEVELS.joinToString("·")} 중 하나") },
                requestId = requestId?.takeIf { it.isNotBlank() }?.also { if (!it.matches(REQUEST_ID)) throw Invalid("request_id 형식이 다르다") },
                text = q?.takeIf { it.isNotEmpty() }?.also {
                    if (it.trim().length < 2) throw Invalid("q 는 2자 이상")
                    if (it.length > 200) throw Invalid("q 는 200자 이하")
                },
                before = before?.takeIf { it.isNotBlank() }?.let { StreamId.parse(it) ?: throw Invalid("before 는 응답의 next_before 값이어야 한다") },
                limit = (limit?.takeIf { it.isNotBlank() } ?: "100").toIntOrNull()?.takeIf { it in 1..MAX_LIMIT } ?: throw Invalid("limit 은 1~$MAX_LIMIT"),
            )
        } catch (error: Invalid) { return invalid(error.detail) }
        return answering("search") { respond(query, reader.search(query)) }
    }

    private fun respond(query: LogQuery, entries: List<LogEntry>): ResponseEntity<out Any> {
        val body = mapOf(
            "logs" to entries.map {
                mapOf("id" to it.id.toString(), "at" to it.id.at.toString(), "level" to it.level, "service" to it.service,
                    "request_id" to it.requestId, "message" to it.message)
            },
            // 꽉 채워 받았으면 더 있을 수 있다 — 마지막 항목의 ID 를 다음 쪽의 기준으로 준다
            "next_before" to (if (entries.size == query.limit) entries.last().id.toString() else ""),
            "from" to query.from.toString(), "to" to query.to.toString(),
        )
        return ResponseEntity.ok(mapOf("success" to true, "data" to body))
    }

    /** 서비스·레벨별 건수(시간 단위). 기간을 주지 않으면 최근 24시간. */
    @GetMapping("/logs/stats")
    fun stats(request: HttpServletRequest, @RequestParam(required = false) from: String?, @RequestParam(required = false) to: String?): ResponseEntity<out Any> =
        stats(request, from, to, Instant.now())

    internal fun stats(request: HttpServletRequest, from: String?, to: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val (start, end) = try { range(from, to, Duration.ofDays(1), MAX_RANGE, now) } catch (error: Invalid) { return invalid(error.detail) }
        return answering("stats") {
            val counts = reader.stats(start, end).map {
                mapOf("hour" to it.hour.toString(), "service" to it.service, "level" to it.level, "count" to it.count)
            }
            ResponseEntity.ok(mapOf("success" to true, "data" to mapOf("counts" to counts, "from" to start.toString(), "to" to end.toString())))
        }
    }

    /** 수집이 살아 있는가 — 알림 서버와 배포가 본다. */
    @GetMapping("/health")
    fun health(): ResponseEntity<Map<String, Any>> {
        val report = health.report(Instant.now())
        return ResponseEntity.status(if (report.healthy) 200 else 503).body(report.body)
    }

    companion object {
        const val MAX_LIMIT = 500
        private const val QUERY_CANCELED = "57014"   // PostgreSQL: statement_timeout 으로 끊김
        val MAX_RANGE: Duration = Duration.ofDays(31)
        val LEVELS = setOf("debug", "info", "warning", "error")
        private val SERVICE = Regex("^[a-z0-9-]{1,40}$")
        private val REQUEST_ID = Regex("^[A-Za-z0-9_.-]{1,80}$")
    }
}
