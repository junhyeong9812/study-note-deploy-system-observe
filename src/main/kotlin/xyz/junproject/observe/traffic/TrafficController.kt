package xyz.junproject.observe.traffic

import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DataAccessException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import xyz.junproject.observe.metric.SeriesPlan
import xyz.junproject.observe.metric.SeriesSource
import java.security.MessageDigest
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * 통신 조회 — 전부 GET, 전부 `X-Observe-Secret`(설정이 비면 전부 거절, 비교는 상수 시간). 인증을 입력 검사보다 먼저 본다.
 * 봉투 · 오류는 지표 조회와 같다: 401 `unauthorized` · 422 `invalid_request`(`detail`) · DB 오류 503 `query_failed` · 시간 초과 504 `query_timeout`.
 * 수치(`statistics`): `count` · `status{2xx,3xx,4xx,5xx,other}` · `error_rate`(5xx ÷ count, count 0 이면 null) · `p50` · `p95`
 * (`{"le_ms": <든 칸의 위쪽 경계>}` 또는 20번째 칸이면 `{"gt_ms": 180000}`, request 줄이 없으면 null — [TrafficDistribution.quantile]) · `max_ms`.
 * 서버(`servers[]`): `{server, read_progress_at(ISO 또는 null), receive_delayed, sequence_gaps, last_received_at, missing[], requests_dropped, requests_rejected,
 * first_seq_seen(또는 null)}` —
 * [TrafficReader.serverProgress] 의 서버와 기간 안에 받은 항목의 서버. 서버 이름순. `missing` · `requests_dropped` · `requests_rejected` 는 기간 안에 받은 항목의 요약.
 *
 * - `GET /traffic/map?from=&to=` → `{from, to, servers, edges}`. 기간: 둘 다 없으면 최근 5분, 최대 24시간(정확히 24시간은 받는다). 원본 칸만 읽는다
 *   (`rawSince = from`). `edges[]`: `(service, caller, class)` 마다 `{service, caller, class, <statistics>, filling_count}` — 수치는 **채워진 칸만**,
 *   "채우는 중"인 칸([TrafficReadProgress.isFilling] — 그 칸의 서버의 읽기 진도로)의 수는 `filling_count` 에만. 서비스 · 호출자 · 갈래순.
 * - `GET /traffic/series?service=&caller=&class=&from=&to=` → `{service, caller, class, from, to, step_ms, source, servers, points}`.
 *   `service` 필수, `caller` 선택(없으면 모든 호출자의 합 — 답의 `caller` 는 null), `class` 기본 `request`. 기간: 둘 다 없으면 최근 1시간, 최대 31일.
 *   간격 · 읽는 곳은 [xyz.junproject.observe.metric.SeriesPlan] 과 같다. `points[]`: 칸이 있는 간격마다 `{at, <statistics>, filling}` — `filling` 은 그 간격에
 *   채우는 중인 칸이 하나라도 있으면 참(수치에는 모든 칸이 든다).
 * - `GET /traffic/requests?request_id=` → `{request_id, spans, truncated}`. `request_id` 필수, 꼴 [REQUEST_ID](로그 조회와 같은 넓은 꼴).
 *   `spans[]`: `{server, service, caller, method, path, path_truncated, status, end_at, duration_ms, upstream_ms, upstream_status, slot, bytes_sent}` —
 *   `end_ms` · 서비스순, [MAX_SPANS] 개까지. 저장소에 하나 더 물어 넘쳤으면 `truncated` 참.
 */
@RestController
class TrafficController(
    private val reader: TrafficReader,
    @Value("\${observe.secret:}") private val secret: String,
) {
    private val log = LoggerFactory.getLogger("observe")

    // 인증 · 422 · DB 오류의 꼴은 지표 조회([xyz.junproject.observe.metric.MetricController])와 같다
    private fun authorized(request: HttpServletRequest): Boolean {
        val provided = request.getHeader("X-Observe-Secret") ?: return false
        return secret.isNotBlank() && MessageDigest.isEqual(provided.toByteArray(), secret.toByteArray())
    }

    private fun unauthorized() = ResponseEntity.status(401).body(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")))
    private fun invalid(detail: String) =
        ResponseEntity.status(422).body(mapOf("success" to false, "error" to mapOf("code" to "invalid_request", "detail" to detail)))
    private fun ok(data: Map<String, Any?>) = ResponseEntity.ok(mapOf("success" to true, "data" to data))

    private class Invalid(val detail: String) : RuntimeException(detail)

    /** DB 오류는 종류 이름만 로그에 — 드라이버 문구에 경로 · 요청 ID 가 실릴 수 있다 */
    private fun answering(what: String, block: () -> ResponseEntity<out Any>): ResponseEntity<out Any> = try {
        block()
    } catch (error: DataAccessException) {
        val timedOut = error is QueryTimeoutException ||
            generateSequence<Throwable>(error) { it.cause }.any { it is SQLException && it.sqlState == QUERY_CANCELED }
        log.error("observe:observe:traffic {} failed{}: {}", what, if (timedOut) " (timeout)" else "", error.javaClass.simpleName)
        val (status, code) = if (timedOut) 504 to "query_timeout" else 503 to "query_failed"
        ResponseEntity.status(status).body(mapOf("success" to false, "error" to mapOf("code" to code)))
    }

    private fun instant(name: String, text: String?): Instant? =
        try { text?.takeIf { it.isNotBlank() }?.let(Instant::parse) } catch (_: DateTimeParseException) { throw Invalid("$name 은 ISO 시각이어야 한다(예: 2026-11-24T00:00:00Z)") }

    /** 기간 — 둘 다 없으면 `[now − 기본, now)`, `to` 만 없으면 지금까지, `from` 만 없으면 `to` 앞의 기본만큼. 거꾸로거나 [maxRange] 를 넘으면 422 */
    private fun range(from: String?, to: String?, now: Instant, defaultRange: Duration, maxRange: Duration): Pair<Instant, Instant> {
        val end = instant("to", to) ?: now
        val start = instant("from", from) ?: end.minus(defaultRange)
        if (start.isAfter(end)) throw Invalid("from 이 to 보다 늦다")
        if (Duration.between(start, end) > maxRange) throw Invalid("기간은 최대 ${maxRange.toHours()}시간")
        return start to end
    }

    private fun name(label: String, text: String?): String? {
        if (text.isNullOrEmpty()) return null
        if (!TrafficEntryParser.NAME.matches(text)) throw Invalid("$label 형식이 다르다")
        return text
    }

    @GetMapping("/traffic/map")
    fun map(
        request: HttpServletRequest,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
    ): ResponseEntity<out Any> = map(request, from, to, Instant.now())

    internal fun map(request: HttpServletRequest, from: String?, to: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val (start, end) = try { range(from, to, now, MAP_DEFAULT_RANGE, MAP_MAX_RANGE) } catch (error: Invalid) { return invalid(error.detail) }
        return answering("map") {
            val progress = reader.serverProgress()
            val readProgress = progress.associate { it.server to it.readProgressAt }
            // 단순화: 지도는 기간(최대 24시간)의 30초 칸을 하나씩 읽어 여기서 합친다 — 서버 넷 · 선 열 개면 24시간에 약 11만 칸. 느려지면 SQL 에서 서버 · 선 · 채우는 중으로 묶는다
            val edges = reader.cells(TrafficCellQuery(start, end, start)).groupBy { Triple(it.service, it.caller, it.trafficClass) }
                .toSortedMap(compareBy({ it.first }, { it.second }, { it.third.wireName }))
                .map { (key, cells) ->
                    val (filling, settled) = cells.partition { TrafficReadProgress.isFilling(it.bucketStart.plus(it.width), readProgress[it.server]) }
                    mapOf("service" to key.first, "caller" to key.second, "class" to key.third.wireName) +
                        statistics(settled.fold(TrafficTotals.EMPTY) { sum, cell -> sum + cell.totals }) +
                        ("filling_count" to filling.sumOf { it.totals.count })
                }
            ok(mapOf("from" to start.toString(), "to" to end.toString(), "servers" to servers(progress, start, end, now), "edges" to edges))
        }
    }

    @GetMapping("/traffic/series")
    fun series(
        request: HttpServletRequest,
        @RequestParam(required = false) service: String?,
        @RequestParam(required = false) caller: String?,
        @RequestParam(name = "class", required = false) trafficClass: String?,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
    ): ResponseEntity<out Any> = series(request, service, caller, trafficClass, from, to, Instant.now())

    internal fun series(request: HttpServletRequest, service: String?, caller: String?, trafficClass: String?, from: String?, to: String?,
                        now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val asked = try {
            val checkedService = name("service", service) ?: throw Invalid("service 가 없다")
            val checkedCaller = name("caller", caller)
            val checkedClass = trafficClass?.takeIf { it.isNotEmpty() }?.let { TrafficClass.of(it) ?: throw Invalid("class 는 request · health · stream 중 하나") }
                ?: TrafficClass.REQUEST
            val (start, end) = range(from, to, now, SERIES_DEFAULT_RANGE, SERIES_MAX_RANGE)
            // 간격 · 읽는 곳은 지표의 시계열과 같다 — 5분 묶음을 읽는 요청은 칸을 5분 경계에서 센다
            val plan = SeriesPlan.choose(start, end, now).let { first ->
                if (first.source == SeriesSource.RAW) first else SeriesPlan.choose(SeriesPlan.floorTo(start, SeriesPlan.ROLLUP_STEP), end, now)
            }
            val origin = if (plan.source == SeriesSource.RAW) start else SeriesPlan.floorTo(start, SeriesPlan.ROLLUP_STEP)
            SeriesAsked(TrafficCellQuery(origin, end, plan.rawSince, checkedService, checkedCaller, checkedClass), plan, start)
        } catch (error: Invalid) { return invalid(error.detail) }
        val (query, plan, start) = asked
        return answering("series") {
            val progress = reader.serverProgress()
            val readProgress = progress.associate { it.server to it.readProgressAt }
            val points = reader.steps(query, query.from, plan.step).groupBy { it.stepStart }.toSortedMap().map { (at, steps) ->
                mapOf("at" to at.toString()) +
                    statistics(steps.fold(TrafficTotals.EMPTY) { sum, step -> sum + step.totals }) +
                    ("filling" to steps.any { TrafficReadProgress.isFilling(it.lastCellEnd, readProgress[it.server]) })
            }
            ok(mapOf(
                "service" to query.service, "caller" to query.caller, "class" to query.trafficClass?.wireName,
                "from" to start.toString(), "to" to query.to.toString(), "step_ms" to plan.step.toMillis(), "source" to plan.source.wireName,
                "servers" to servers(progress, start, query.to, now), "points" to points,
            ))
        }
    }

    @GetMapping("/traffic/requests")
    fun requests(
        request: HttpServletRequest,
        @RequestParam(name = "request_id", required = false) requestId: String?,
    ): ResponseEntity<out Any> = requests(request, requestId, Instant.now())

    internal fun requests(request: HttpServletRequest, requestId: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        if (requestId.isNullOrEmpty()) return invalid("request_id 가 없다")
        if (!REQUEST_ID.matches(requestId)) return invalid("request_id 형식이 다르다")
        return answering("requests") {
            val found = reader.requestSpans(requestId, MAX_SPANS + 1)
            val spans = found.take(MAX_SPANS).map { (server, line) ->
                mapOf(
                    "server" to server, "service" to line.service, "caller" to line.caller, "method" to line.method, "path" to line.path,
                    "path_truncated" to line.pathTruncated, "status" to line.status, "end_at" to line.endAt.toString(), "duration_ms" to line.durationMs,
                    "upstream_ms" to line.upstreamMs, "upstream_status" to line.upstreamStatus, "slot" to line.slot, "bytes_sent" to line.bytesSent,
                )
            }
            ok(mapOf("request_id" to requestId, "spans" to spans, "truncated" to (found.size > MAX_SPANS)))
        }
    }

    /** 시계열 질의를 검사한 결과 — 저장소에 물을 칸([query] 의 `from` 이 간격의 시작)과 간격, 답에 싣는 요청 기간의 시작 */
    private data class SeriesAsked(val query: TrafficCellQuery, val plan: SeriesPlan, val start: Instant)

    /** 수치 — 오류율은 5xx ÷ count(4xx · 499 는 오류가 아니다), 분위수는 request 줄이 없으면 null */
    private fun statistics(totals: TrafficTotals): Map<String, Any?> = mapOf(
        "count" to totals.count,
        "status" to mapOf("2xx" to totals.status.status2xx, "3xx" to totals.status.status3xx, "4xx" to totals.status.status4xx,
            "5xx" to totals.status.status5xx, "other" to totals.status.other),
        "error_rate" to (if (totals.count == 0L) null else totals.status.status5xx.toDouble() / totals.count),
        "p50" to quantile(totals.buckets, 50),
        "p95" to quantile(totals.buckets, 95),
        "max_ms" to totals.maxMs,
    )

    private fun quantile(buckets: List<Long>, percent: Int): Map<String, Long>? = TrafficDistribution.quantile(buckets, percent)?.let {
        if (it.atMostMs != null) mapOf("le_ms" to it.atMostMs) else mapOf("gt_ms" to requireNotNull(it.overMs))
    }

    /** 서버마다의 수신 상태 — 진도를 아는 서버와 기간 안에 받은 항목의 서버, 이름순 */
    private fun servers(progress: List<TrafficServerProgress>, from: Instant, to: Instant, now: Instant): List<Map<String, Any?>> {
        val summaries = reader.entrySummaries(from, to)
        val byServer = progress.associateBy { it.server }
        return (byServer.keys + summaries.keys).sorted().map { server ->
            val known = byServer[server]
            val summary = summaries[server]
            mapOf(
                "server" to server,
                "read_progress_at" to known?.readProgressAt?.toString(),
                "receive_delayed" to TrafficReadProgress.isReceiveDelayed(known?.progressAdvancedAt, now),
                "sequence_gaps" to (known?.sequenceGaps ?: 0L),
                "last_received_at" to known?.lastReceivedAt?.toString(),
                "missing" to summary?.missing.orEmpty(),
                "requests_dropped" to (summary?.requestsDropped ?: 0L),
                "requests_rejected" to (summary?.requestsRejected ?: 0L),
                "first_seq_seen" to known?.firstSeqSeen,
            )
        }
    }

    companion object {
        private const val QUERY_CANCELED = "57014"   // PostgreSQL: statement_timeout 으로 끊김
        val MAP_DEFAULT_RANGE: Duration = Duration.ofMinutes(5)
        val MAP_MAX_RANGE: Duration = Duration.ofHours(24)
        val SERIES_DEFAULT_RANGE: Duration = Duration.ofHours(1)
        val SERIES_MAX_RANGE: Duration = Duration.ofDays(31)
        const val MAX_SPANS = 500
        /** 조회하는 요청 ID 의 꼴 — 로그 조회와 같은 넓은 꼴(저장된 로그의 어떤 ID 로도 물을 수 있게) */
        val REQUEST_ID = Regex("^[A-Za-z0-9_.-]{1,80}$")
    }
}
