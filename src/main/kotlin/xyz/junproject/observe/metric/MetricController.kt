package xyz.junproject.observe.metric

import jakarta.servlet.http.HttpServletRequest
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.time.Instant

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
 *   - `firing`: 이번 task 에서는 늘 빈 배열(임계값 판정은 task 04).
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
) {
    private fun notImplemented(): ResponseEntity<out Any> =
        ResponseEntity.status(501).body(mapOf("success" to false, "error" to mapOf("code" to "not_implemented")))

    @GetMapping("/metrics/latest")
    fun latest(request: HttpServletRequest): ResponseEntity<out Any> = latest(request, Instant.now())

    internal fun latest(request: HttpServletRequest, now: Instant): ResponseEntity<out Any> = notImplemented()

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
                        now: Instant): ResponseEntity<out Any> = notImplemented()

    @GetMapping("/metrics/containers")
    fun containers(
        request: HttpServletRequest,
        @RequestParam(required = false) server: String?,
        @RequestParam(required = false) at: String?,
    ): ResponseEntity<out Any> = containers(request, server, at, Instant.now())

    internal fun containers(request: HttpServletRequest, server: String?, at: String?, now: Instant): ResponseEntity<out Any> = notImplemented()

    companion object {
        val MAX_RANGE: Duration = Duration.ofDays(31)
        val DEFAULT_RANGE: Duration = Duration.ofHours(1)
        val LATEST_WINDOW: Duration = Duration.ofHours(24)
        val STALE_AFTER: Duration = Duration.ofMinutes(3)
        val CONTAINERS_WINDOW: Duration = Duration.ofHours(24)
        val CONTAINERS_AROUND: Duration = Duration.ofMinutes(5)
    }
}
