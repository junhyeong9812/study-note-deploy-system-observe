package xyz.junproject.observe.visit

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.security.MessageDigest
import java.sql.SQLException
import java.time.Instant

/**
 * 방문 요약 조회 `GET /visits/summary?days=` — `/logs` · `/metrics/…` 와 같은 봉투와 인증:
 * `X-Observe-Secret`(입력 검사보다 먼저, 설정이 비면 전부 거절, 비교는 상수 시간) · 401 `unauthorized` · 422 `invalid_request`(`detail`) ·
 * DB 오류 503 `query_failed` · 시간 초과 504 `query_timeout`. `days` 는 7 · 30 · 90 만(없으면 7).
 *
 * 응답 `data`: `{days, timezone, from, to, daily:[{date, views, views_with_bots, visitors, visitors_with_bots}],
 * top_paths:{items:[{path, views}], truncated}, top_referrers:{items:[{host, views}], truncated}, devices:{desktop, mobile, bot}, bot_ratio}`.
 * 요약 수치만 — 주소 · 구분값은 싣지 않는다. `bot_ratio` = 봇 조회 수 ÷ 전체 조회 수(없으면 0).
 */
@RestController
class VisitController(private val reader: VisitReader, @Value("\${observe.secret:}") private val secret: String) {
    private val log = LoggerFactory.getLogger("observe")

    @GetMapping("/visits/summary")
    fun summary(request: HttpServletRequest, @RequestParam(required = false) days: String?): ResponseEntity<out Any> = summary(request, days, Instant.now())

    internal fun summary(request: HttpServletRequest, days: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return ResponseEntity.status(401).body(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")))
        val window = if (days == null) DEFAULT_DAYS else VisitStore.SUMMARY_DAYS.firstOrNull { it.toString() == days }
            ?: return ResponseEntity.status(422).body(mapOf("success" to false,
                "error" to mapOf("code" to "invalid_request", "detail" to "days 는 ${VisitStore.SUMMARY_DAYS.joinToString(" · ")} 중 하나")))
        return try {
            ResponseEntity.ok(mapOf("success" to true, "data" to data(reader.summary(window, now))))
        } catch (error: DataAccessException) {
            val sqlState = generateSequence<Throwable>(error) { it.cause }.filterIsInstance<SQLException>().firstNotNullOfOrNull { it.sqlState }
            val timedOut = error is QueryTimeoutException || sqlState == QUERY_CANCELED
            // 예외 글은 남기지 않는다 — 방문 표의 오류 글에는 줄의 값이 실릴 수 있다. 종류와 SQLSTATE 만
            log.error("observe:observe:visit summary failed{}: {} (SQLSTATE {})", if (timedOut) " (timeout)" else "", error.javaClass.simpleName, sqlState ?: "-")
            val (status, code) = if (timedOut) 504 to "query_timeout" else 503 to "query_failed"
            ResponseEntity.status(status).body(mapOf("success" to false, "error" to mapOf("code" to code)))
        }
    }

    private fun authorized(request: HttpServletRequest): Boolean {
        val provided = request.getHeader("X-Observe-Secret") ?: return false
        return secret.isNotBlank() && MessageDigest.isEqual(provided.toByteArray(), secret.toByteArray())
    }

    private fun data(summary: VisitSummary): Map<String, Any?> {
        val total = summary.devices.values.sum()
        return mapOf(
            "days" to summary.days,
            "timezone" to VisitStore.ZONE.id,
            "from" to summary.from.toString(),
            "to" to summary.to.toString(),
            "daily" to summary.daily.map {
                mapOf("date" to it.date.toString(), "views" to it.views, "views_with_bots" to it.viewsWithBots,
                    "visitors" to it.visitors, "visitors_with_bots" to it.visitorsWithBots)
            },
            "top_paths" to mapOf("items" to summary.topPaths.map { mapOf("path" to it.path, "views" to it.views) }, "truncated" to summary.topPathsTruncated),
            "top_referrers" to mapOf("items" to summary.topReferrers.map { mapOf("host" to it.host, "views" to it.views) }, "truncated" to summary.topReferrersTruncated),
            "devices" to VisitDevice.entries.associate { it.wireName to (summary.devices[it] ?: 0L) },
            "bot_ratio" to if (total == 0L) 0.0 else (summary.devices[VisitDevice.BOT] ?: 0L).toDouble() / total,
        )
    }

    private companion object {
        const val DEFAULT_DAYS = 7
        const val QUERY_CANCELED = "57014"   // PostgreSQL: statement_timeout 으로 끊김
    }
}
