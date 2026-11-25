package xyz.junproject.observe.traffic

import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.metric.RollupOutcome
import java.time.Duration
import java.time.Instant

/** 항목 하나를 저장한 결과 — [duplicate] 면 이미 받은 순번이라 통째로 버렸다(나머지 수는 0) */
data class TrafficStoreOutcome(
    val duplicate: Boolean,
    val segmentsStored: Int = 0,
    val segmentsAhead: Int = 0,
    val segmentsTooOld: Int = 0,
    val requestsStored: Int = 0,
    val requestsAhead: Int = 0,
    val requestsTooOld: Int = 0,
)

/** 수집 쪽이 쓰는 면 */
interface TrafficSink {
    /** 표를 만든다(있으면 그대로) — 추가만, 기존 표는 건드리지 않는다. 실패는 예외 */
    fun createSchema()
    /** 항목 하나를 한 트랜잭션으로 저장한다(규약 4.5). 실패는 예외 — 아무것도 남지 않는다 */
    fun store(entry: TrafficEntry): TrafficStoreOutcome
}

/** 칸 하나의 값(원본 행들의 합) — [width] 는 원본이면 30초, 5분 묶음이면 5분 */
data class TrafficCell(
    val server: String,
    val bucketStart: Instant,
    val width: Duration,
    val service: String,
    val caller: String,
    val trafficClass: TrafficClass,
    val totals: TrafficTotals,
)

/** 칸 조회 — 칸 시작이 `[from, to)`. 칸 시작이 [rawSince] 보다 앞선 부분은 5분 묶음에서, 나머지는 원본에서. 거르개가 null 이면 모두 */
data class TrafficCellQuery(
    val from: Instant,
    val to: Instant,
    val rawSince: Instant,
    val service: String? = null,
    val caller: String? = null,
    val trafficClass: TrafficClass? = null,
)

/** 기간 안에 받은 항목의 요약(서버마다) — `missing` 은 겹침 없이 글자순, 요청별 줄을 상한으로 싣지 못한 수의 합 */
data class TrafficEntrySummary(val missing: List<String>, val requestsDropped: Long, val requestsRejected: Long = 0)

/** 요청 구간 하나 — 받은 서버와 요청별 줄 */
data class TrafficSpan(val server: String, val request: TrafficRequest)

/** 조회 쪽이 쓰는 면 */
interface TrafficReader {
    /** 칸마다의 값 — 같은 `(server, bucket_start, service, caller, class)` 의 원본 행을 합친 것. 서버 · 칸 시작 · 서비스 · 호출자 · 갈래순 */
    fun cells(query: TrafficCellQuery): List<TrafficCell>
    /** 서버마다의 읽기 진도 · 틈 · 마지막 수신 — 서버 이름순 */
    fun serverProgress(): List<TrafficServerProgress>
    /** 받은 시각이 `[from, to)` 인 항목의 요약 — 서버 이름 → 요약 */
    fun entrySummaries(from: Instant, to: Instant): Map<String, TrafficEntrySummary>
    /** 요청 ID 하나의 구간 — `end_ms` · 서비스순, 많아야 [limit] 개 */
    fun requestSpans(requestId: String, limit: Int): List<TrafficSpan>
}

/** 원본 7일 · 5분 묶음 30일 삭제 한 번의 결과 */
data class TrafficDeletion(val entriesDeleted: Int, val segmentsDeleted: Int, val requestsDeleted: Int, val rollupDeleted: Int)

/** 통신 삭제 한 번의 결과(헬스용) */
data class TrafficRetentionResult(val ranAt: Instant, val deletion: TrafficDeletion?, val failure: String?)

/** 통신 5분 묶음 한 바퀴의 결과(헬스용) */
data class TrafficRollupResult(val ranAt: Instant, val buckets: Int?, val failure: String?, val failedBuckets: Int = 0)

/** 예약 실행이 쓰는 면 — 자원 지표의 [xyz.junproject.observe.metric.MetricMaintenance] 와 같은 규칙 */
interface TrafficMaintenance {
    fun rollUp(now: Instant): Int
    fun rollUpBuckets(now: Instant): RollupOutcome = RollupOutcome(rollUp(now), 0)
    fun deleteExpired(now: Instant): TrafficDeletion
}

/**
 * PostgreSQL 저장소 — 로그 · 지표와 같은 DB, 표는 추가만(기존 표는 건드리지 않는다). 수집 · 조회 · 헬스의 연결 풀을 나눈다.
 * 표의 이름과 칸(계약 — 엔진 테스트가 이 이름으로 본다):
 * - `traffic_receipt`: 받은 열쇠 — 기본 키 `(server, generation, seq)`. 칸 `read_ms`(항목의 `at`) · `received_ms` · `requests_dropped` · `requests_rejected` · `missing text[]`.
 *   [store] 의 트랜잭션의 **첫 문장**이 이 표의 `INSERT … ON CONFLICT (server, generation, seq) DO NOTHING` 이다. 넣은 행이 0 이면 그 항목은 통째로 버린다.
 * - `traffic_segment`: 구간 묶음의 원본 행 — **추가만**(칸 한 줄을 UPDATE 로 고치지 않는다). 칸 `server` · `bucket_ms` · `service` · `caller` · `class` · `count` ·
 *   `status_2xx` … `status_other` · `buckets` · `max_ms` · 어느 항목의 것인지(`generation` · `seq`) · `received_ms` · `insert_order`(DB 가 매기는 저장 순번).
 *   칸의 값은 읽을 때 같은 `(server, bucket_ms, service, caller, class)` 의 행을 합쳐 낸다.
 * - `traffic_request`: 요청별 줄(요청 ID 로 찾는다).
 * - 5분 묶음 · 그 계산 기록 · 시도 시각의 표, `(server, generation)` 마다의 순번 요약의 표 — 이름은 `traffic_` 로 시작한다(그 밖은 구현이 정한다).
 * 저장된 것 어디에도 접속 주소 · 쿼리 · 모르는 칸의 값을 넣지 않는다.
 *
 * - [store]: 한 항목 = 한 트랜잭션. 순번 → 칸마다의 앞섬 · 오래됨([TrafficCellAge] — 묶음은 `bucket_start_ms`, 요청별 줄은 `end_ms`) 차례로 본다.
 *   버린 칸이 있어도 열쇠는 "받음"으로 남는다. 순번 요약(가장 큰 순번 · 받은 수 · 틈 없이 이어진 끝과 그 `at` · 끝이 나아간 받은 시각)도 같은 트랜잭션에서.
 * - [serverProgress]: [TrafficServerProgress] 의 규칙(처음 본 순번 · 틈 기다림 · 0 하한). 마지막 수신 = 새로 받은 항목의 가장 늦은 받은 시각(다시 온 순번은 치지 않는다).
 * - [entrySummaries]: `requestsRejected` 는 기간 안에 받은 항목의 [TrafficEntry.requestsRejected] 합.
 * - 5분 묶음 · 삭제: 자원 지표([xyz.junproject.observe.metric.MetricStore])와 같은 방식 — 끝난 5분 구간마다 원본에서 다시 계산(늦게 저장된 원본이 있으면 다시),
 *   원본 칸은 그 구간 전체가 `now − 7일` 앞이고 5분 묶음이 원본을 모두 담은 구간만 지운다. 요청별 줄은 `end_ms` 가, 받은 열쇠는 `received_ms` 가 `now − 7일` 앞이면 지운다.
 *   5분 묶음은 구간 시작이 `now − 30일` 앞이면 지운다. 경계(정확히 7일 · 30일)는 남긴다.
 * 뼈대: 본문은 구현 워커가 채운다.
 */
class TrafficStore(
    private val ingest: JdbcTemplate,
    private val query: JdbcTemplate,
    private val health: JdbcTemplate,
    /** 헬스의 선택 칸(서버마다의 진도)을 읽는 연결 — 상태를 정하는 읽기([latestStoredAt])와 풀을 나눈다. 없으면 [health] */
    private val healthDetail: JdbcTemplate = health,
    /** 틈난 순번을 "잃음"으로 확정하기까지 기다리는 시간([TrafficServerProgress] 의 규칙) */
    private val gapWait: Duration = DEFAULT_GAP_WAIT,
) : TrafficSink, TrafficReader, TrafficMaintenance {
    override fun createSchema(): Unit = TODO("뼈대: 구현 워커가 채운다")

    /** 뜬 직후 한 번 미리 만든다 — 실패는 예외 */
    fun prepareSchema(): Unit = TODO("뼈대: 구현 워커가 채운다")

    override fun store(entry: TrafficEntry): TrafficStoreOutcome = TODO("뼈대: 구현 워커가 채운다")

    override fun cells(query: TrafficCellQuery): List<TrafficCell> = TODO("뼈대: 구현 워커가 채운다")

    override fun serverProgress(): List<TrafficServerProgress> = TODO("뼈대: 구현 워커가 채운다")

    /** 헬스용 — 헬스의 선택 칸 연결로 읽고 표를 만들지 않는다 */
    fun healthProgress(): List<TrafficServerProgress> = TODO("뼈대: 구현 워커가 채운다")

    override fun entrySummaries(from: Instant, to: Instant): Map<String, TrafficEntrySummary> = TODO("뼈대: 구현 워커가 채운다")

    override fun requestSpans(requestId: String, limit: Int): List<TrafficSpan> = TODO("뼈대: 구현 워커가 채운다")

    /** 가장 최근에 받은 항목의 받은 시각(헬스용) — 없으면 null, 표가 아직 없으면 SQLSTATE 42P01 예외 */
    fun latestStoredAt(): Instant? = TODO("뼈대: 구현 워커가 채운다")

    override fun rollUp(now: Instant): Int = TODO("뼈대: 구현 워커가 채운다")

    override fun rollUpBuckets(now: Instant): RollupOutcome = TODO("뼈대: 구현 워커가 채운다")

    override fun deleteExpired(now: Instant): TrafficDeletion = TODO("뼈대: 구현 워커가 채운다")

    companion object {
        val RAW_RETENTION: Duration = Duration.ofDays(7)
        val ROLLUP_RETENTION: Duration = Duration.ofDays(30)
        val ROLLUP: Duration = Duration.ofMinutes(5)
        val CELL: Duration = Duration.ofSeconds(30)
        const val BUCKETS_PER_ROUND = 200
        val DEFAULT_GAP_WAIT: Duration = Duration.ofMinutes(10)
    }
}
