package xyz.junproject.observe.traffic

import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import java.time.Duration
import java.time.Instant

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
 * 뼈대: 본문은 구현 워커가 채운다. 구현에서 `@RestController` 와 경로마다 `@GetMapping`(질의 이름 `from` · `to` · `service` · `caller` · `class` · `request_id`)을 단다 —
 * 뼈대에 달면 저장소 빈이 없어 앱이 뜨지 않는다.
 */
class TrafficController(
    private val reader: TrafficReader,
    private val secret: String,
) {
    internal fun map(request: HttpServletRequest, from: String?, to: String?, now: Instant): ResponseEntity<out Any> = TODO("뼈대: 구현 워커가 채운다")

    internal fun series(request: HttpServletRequest, service: String?, caller: String?, trafficClass: String?, from: String?, to: String?,
                        now: Instant): ResponseEntity<out Any> = TODO("뼈대: 구현 워커가 채운다")

    internal fun requests(request: HttpServletRequest, requestId: String?, now: Instant): ResponseEntity<out Any> = TODO("뼈대: 구현 워커가 채운다")

    companion object {
        val MAP_DEFAULT_RANGE: Duration = Duration.ofMinutes(5)
        val MAP_MAX_RANGE: Duration = Duration.ofHours(24)
        val SERIES_DEFAULT_RANGE: Duration = Duration.ofHours(1)
        val SERIES_MAX_RANGE: Duration = Duration.ofDays(31)
        const val MAX_SPANS = 500
        /** 조회하는 요청 ID 의 꼴 — 로그 조회와 같은 넓은 꼴(저장된 로그의 어떤 ID 로도 물을 수 있게) */
        val REQUEST_ID = Regex("^[A-Za-z0-9_.-]{1,80}$")
    }
}
