package xyz.junproject.observe.metric

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
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException
import xyz.junproject.observe.threshold.FiringSource

/**
 * 지표 조회(§10.3 · §11.5 · §11.6) — 전부 GET, 전부 `X-Observe-Secret`(설정이 비면 전부 거절, 비교는 상수 시간).
 * 인증을 입력 검사보다 먼저 본다. 봉투는 `{success, data}` / `{success:false, error:{code}}` — `/logs` 와 같다:
 * 401 `unauthorized` · 422 `invalid_request`(`detail`) · DB 오류 503 `query_failed` · 시간 초과 504 `query_timeout`.
 *
 * - `GET /metrics/latest` → `{servers:[{server, at, received_at, age_ms, stale, host, containers, gpus, missing}], firing:[]}`
 *   - 서버: 수신 시각이 최근 24시간 안인 서버 + [expectedServers](쉼표로 나눈 이름 — `METRICS_EXPECTED_SERVERS`) 중 그 밖의 서버. 이름순.
 *   - `at`: 그 서버의 가장 최근 항목의 측정 시각, `received_at`: 그 항목의 수신 시각, `age_ms` = now − received_at, `stale` = age_ms > 3분.
 *   - 24시간 안에 받은 것이 없는 기대 서버("수신 없음"): `at`·`received_at`·`age_ms`·`host` 는 null, `stale` true, `containers`·`gpus`·`missing` 는 빈 배열.
 *   - `host`: 스트림 `body.host` 와 같은 칸 이름(없으면 null). `containers[]`: `{name, series_name, id, project, cpu_pct, mem_used, restarts, state}`
 *     (`series_name` = 자리 이름을 뗀 이름). `gpus[]`: `{index, util_pct, mem_used, mem_total, temp_c}`. 값이 없으면 JSON null.
 *   - `firing`: 지금 넘어 있는 규칙 `[{server, rule, target, severity, since, last_value}]`(판정을 붙이지 않았으면 빈 배열).
 * - `GET /metrics/series?server=&scope=&name=&from=&to=` → `{server, scope, name, from, to, step_ms, source, points}`
 *   - `server` 필수([MetricEntryParser.SERVER_NAME] 꼴). `scope` 필수(host·container·gpu). `name`: host 는 주면 422,
 *     container 는 필수(`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`), gpu 는 필수(숫자 1~3자리).
 *   - 기간: 둘 다 없으면 최근 1시간, `from` 만 있으면 지금까지, `to` 만 있으면 그 앞 1시간. ISO 시각. 거꾸로면 422, 31일 초과 422(정확히 31일은 받는다).
 *   - 간격·읽는 곳은 [SeriesPlan.choose]. `name` 은 host 면 null.
 *   - `points[]`: `{at, <값>, <값>_max, …}` — 그 갈래의 값([MetricScope.values]) 전부, 표본이 없는 값은 null. 점이 없는 칸은 없다.
 * - `GET /metrics/containers?server=&at=` → `{server, from, to, containers:[{name, last_seen_at}]}`
 *   - `at` 이 없으면 `[now − 24시간, now]`, 있으면 `[at − 5분, at + 5분]`(양 끝 포함). `name` 은 자리 이름을 뗀 것.
 */
@RestController
class MetricController(
    private val reader: MetricReader,
    @Value("\${observe.secret:}") private val secret: String,
    @Value("\${observe.metrics.expected-servers:}") private val expectedServers: String,
    /** 지금 넘어 있는 규칙 — 있으면 `firing` 에 싣는다. 없으면 늘 빈 배열 */
    private val firingSource: FiringSource? = null,
) {
    private val log = LoggerFactory.getLogger("observe")
    private val expected: List<String> = expectedServers.split(',').map { it.trim() }.filter { it.isNotEmpty() }.distinct()

    // 인증 · 422 · DB 오류의 꼴은 `/logs`([xyz.junproject.observe.api.LogController])와 같다
    private fun authorized(request: HttpServletRequest): Boolean {
        val provided = request.getHeader("X-Observe-Secret") ?: return false
        return secret.isNotBlank() && MessageDigest.isEqual(provided.toByteArray(), secret.toByteArray())
    }

    private fun unauthorized() = ResponseEntity.status(401).body(mapOf("success" to false, "error" to mapOf("code" to "unauthorized")))
    private fun invalid(detail: String) =
        ResponseEntity.status(422).body(mapOf("success" to false, "error" to mapOf("code" to "invalid_request", "detail" to detail)))
    private fun ok(data: Map<String, Any?>) = ResponseEntity.ok(mapOf("success" to true, "data" to data))

    private class Invalid(val detail: String) : RuntimeException(detail)

    private fun answering(what: String, block: () -> ResponseEntity<out Any>): ResponseEntity<out Any> = try {
        block()
    } catch (error: DataAccessException) {
        val timedOut = error is QueryTimeoutException ||
            generateSequence<Throwable>(error) { it.cause }.any { it is SQLException && it.sqlState == QUERY_CANCELED }
        log.error("observe:observe:metric {} failed{}: {}: {}", what, if (timedOut) " (timeout)" else "", error.javaClass.simpleName, error.message?.take(500))
        val (status, code) = if (timedOut) 504 to "query_timeout" else 503 to "query_failed"
        ResponseEntity.status(status).body(mapOf("success" to false, "error" to mapOf("code" to code)))
    }

    private fun instant(name: String, text: String?): Instant? =
        try { text?.takeIf { it.isNotBlank() }?.let(Instant::parse) } catch (_: DateTimeParseException) { throw Invalid("$name 은 ISO 시각이어야 한다(예: 2026-11-24T00:00:00Z)") }

    private fun server(text: String?): String {
        if (text.isNullOrEmpty()) throw Invalid("server 가 없다")
        if (!MetricEntryParser.SERVER_NAME.matches(text)) throw Invalid("server 형식이 다르다")
        return text
    }

    @GetMapping("/metrics/latest")
    fun latest(request: HttpServletRequest): ResponseEntity<out Any> = latest(request, Instant.now())

    internal fun latest(request: HttpServletRequest, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        return answering("latest") {
            val received = reader.latest(now.minus(LATEST_WINDOW)).associateBy { it.server }
            val servers = (received.keys + expected).sorted().map { name -> received[name]?.let { latestServer(it, now) } ?: silentServer(name) }
            val firing = firingSource?.firing().orEmpty().map {
                mapOf("server" to it.server, "rule" to it.rule, "target" to it.target, "severity" to it.severity,
                    "since" to it.since.toString(), "last_value" to it.lastValue)
            }
            ok(mapOf("servers" to servers, "firing" to firing))
        }
    }

    private fun latestServer(sample: ResourceSample, now: Instant): Map<String, Any?> {
        val age = Duration.between(sample.receivedAt, now)
        return mapOf(
            "server" to sample.server,
            "at" to sample.measuredAt.toString(),
            "received_at" to sample.receivedAt.toString(),
            "age_ms" to age.toMillis(),
            "stale" to (age > STALE_AFTER),
            "host" to sample.host?.let {
                mapOf("cpu_pct" to it.cpuPct, "load1" to it.load1, "mem_total" to it.memTotal, "mem_used" to it.memUsed,
                    "disk_total" to it.diskTotal, "disk_used" to it.diskUsed, "net_rx_bps" to it.netRxBps, "net_tx_bps" to it.netTxBps,
                    "io_read_bps" to it.ioReadBps, "io_write_bps" to it.ioWriteBps)
            },
            "containers" to sample.containers.map {
                mapOf("name" to it.name, "series_name" to ContainerName.seriesName(it.name), "id" to it.id, "project" to it.project,
                    "cpu_pct" to it.cpuPct, "mem_used" to it.memUsed, "restarts" to it.restarts, "state" to it.state)
            },
            "gpus" to sample.gpus.map {
                mapOf("index" to it.index, "util_pct" to it.utilPct, "mem_used" to it.memUsed, "mem_total" to it.memTotal, "temp_c" to it.tempC)
            },
            "missing" to sample.missing,
        )
    }

    /** 기대 서버인데 24시간 안에 받은 것이 없다("수신 없음") */
    private fun silentServer(name: String): Map<String, Any?> = mapOf(
        "server" to name, "at" to null, "received_at" to null, "age_ms" to null, "stale" to true,
        "host" to null, "containers" to emptyList<Any>(), "gpus" to emptyList<Any>(), "missing" to emptyList<Any>(),
    )

    @GetMapping("/metrics/series")
    fun series(
        request: HttpServletRequest,
        @RequestParam(required = false) server: String?,
        @RequestParam(required = false) scope: String?,
        @RequestParam(required = false) name: String?,
        @RequestParam(required = false) from: String?,
        @RequestParam(required = false) to: String?,
    ): ResponseEntity<out Any> = series(request, server, scope, name, from, to, Instant.now())

    internal fun series(request: HttpServletRequest, server: String?, scope: String?, name: String?, from: String?, to: String?,
                        now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val (query, plan) = try {
            val checkedServer = server(server)
            val checkedScope = scope?.takeIf { it.isNotEmpty() }?.let { MetricScope.of(it) ?: throw Invalid("scope 는 host·container·gpu 중 하나") }
                ?: throw Invalid("scope 가 없다")
            val checkedName = when (checkedScope) {
                MetricScope.HOST -> if (name.isNullOrEmpty()) null else throw Invalid("host 에는 name 을 주지 않는다")
                MetricScope.CONTAINER -> name?.takeIf { CONTAINER_NAME.matches(it) } ?: throw Invalid("container 는 name(자리 이름을 뗀 컨테이너 이름)이 필요하다")
                MetricScope.GPU -> name?.takeIf { GPU_NAME.matches(it) } ?: throw Invalid("gpu 는 name(번호 1~3자리)이 필요하다")
            }
            val end = instant("to", to) ?: now
            val start = instant("from", from) ?: end.minus(DEFAULT_RANGE)
            if (start.isAfter(end)) throw Invalid("from 이 to 보다 늦다")
            if (Duration.between(start, end) > MAX_RANGE) throw Invalid("기간은 최대 ${MAX_RANGE.toDays()}일")
            val plan = SeriesPlan.choose(start, end, now)
            SeriesQuery(checkedServer, checkedScope, checkedName, start, end, plan.step, plan.rawSince) to plan
        } catch (error: Invalid) { return invalid(error.detail) }
        return answering("series") {
            val points = reader.series(query).map { point ->
                buildMap<String, Any?> {
                    put("at", point.at.toString())
                    for (value in query.scope.values) {
                        val aggregate = point.values[value]
                        put(value, aggregate?.average)
                        put("${value}_max", aggregate?.maximum)
                    }
                }
            }
            ok(mapOf(
                "server" to query.server, "scope" to query.scope.wireName, "name" to query.name,
                "from" to query.from.toString(), "to" to query.to.toString(),
                "step_ms" to plan.step.toMillis(), "source" to plan.source.wireName, "points" to points,
            ))
        }
    }

    @GetMapping("/metrics/containers")
    fun containers(
        request: HttpServletRequest,
        @RequestParam(required = false) server: String?,
        @RequestParam(required = false) at: String?,
    ): ResponseEntity<out Any> = containers(request, server, at, Instant.now())

    internal fun containers(request: HttpServletRequest, server: String?, at: String?, now: Instant): ResponseEntity<out Any> {
        if (!authorized(request)) return unauthorized()
        val (checkedServer, window) = try {
            val checkedServer = server(server)
            val around = instant("at", at)
            checkedServer to (if (around == null) now.minus(CONTAINERS_WINDOW) to now else around.minus(CONTAINERS_AROUND) to around.plus(CONTAINERS_AROUND))
        } catch (error: Invalid) { return invalid(error.detail) }
        val (start, end) = window
        return answering("containers") {
            val seen = reader.containers(checkedServer, start, end).map { mapOf("name" to it.name, "last_seen_at" to it.lastSeenAt.toString()) }
            ok(mapOf("server" to checkedServer, "from" to start.toString(), "to" to end.toString(), "containers" to seen))
        }
    }

    companion object {
        val MAX_RANGE: Duration = Duration.ofDays(31)
        val DEFAULT_RANGE: Duration = Duration.ofHours(1)
        val LATEST_WINDOW: Duration = Duration.ofHours(24)
        val STALE_AFTER: Duration = Duration.ofMinutes(3)
        val CONTAINERS_WINDOW: Duration = Duration.ofHours(24)
        val CONTAINERS_AROUND: Duration = Duration.ofMinutes(5)
        private const val QUERY_CANCELED = "57014"   // PostgreSQL: statement_timeout 으로 끊김
        private val CONTAINER_NAME = Regex("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
        private val GPU_NAME = Regex("^[0-9]{1,3}$")
    }
}
