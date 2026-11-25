package xyz.junproject.observe.traffic

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import xyz.junproject.observe.metric.RollupOutcome
import xyz.junproject.observe.metric.SeriesPlan
import xyz.junproject.observe.metric.lastInsertOrder
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
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

    /**
     * 지도용 — 칸 시작이 `[from, to)` 인 원본 칸을 (서버 · 서비스 · 호출자 · 갈래 · 채우는 중)마다 합친 것. 채우는 중의 경계는 칸마다 그 칸의 서버의 읽기 진도
     * ([serverProgress] 의 `readProgressAt`, 없으면 모두 채우는 중)다. 기본은 [cells] 를 읽어 합치고, 저장소는 같은 합을 SQL 에서 낸다(돌아오는 행이 칸이 아니라 선에 비례하게).
     */
    fun edgeTotals(from: Instant, to: Instant): List<TrafficEdgeTotals> {
        val readProgress = serverProgress().associate { it.server to it.readProgressAt }
        return cells(TrafficCellQuery(from, to, from))
            .groupBy { cell -> TrafficEdgeTotals(cell.server, cell.service, cell.caller, cell.trafficClass,
                TrafficReadProgress.isFilling(cell.bucketStart.plus(cell.width), readProgress[cell.server]), TrafficTotals.EMPTY) }
            .map { (key, cells) -> key.copy(totals = cells.fold(TrafficTotals.EMPTY) { sum, cell -> sum + cell.totals }) }
    }

    /**
     * 시계열용 — [cells] 를 [origin] 에서 시작하는 [step] 간격으로 서버마다 합친 것(호출자는 합친다 · 서버 · 간격순). 기본은 [cells] 를 읽어 합치고,
     * 저장소는 같은 합을 SQL 에서 낸다(긴 기간의 칸을 하나씩 읽어 오지 않게).
     */
    fun steps(query: TrafficCellQuery, origin: Instant, step: Duration): List<TrafficStepTotals> =
        cells(query).groupBy { it.server to Math.floorDiv(Duration.between(origin, it.bucketStart).toMillis(), step.toMillis()) }
            .map { (key, cells) ->
                TrafficStepTotals(key.first, origin.plus(step.multipliedBy(key.second)), cells.maxOf { it.bucketStart.plus(it.width) },
                    cells.fold(TrafficTotals.EMPTY) { sum, cell -> sum + cell.totals })
            }
            .sortedWith(compareBy({ it.server }, { it.stepStart }))
}

/**
 * 지도의 선 하나를 서버 · "채우는 중" 여부로 나눈 합 — [filling] 은 그 칸들이 그 서버의 읽기 진도로 "채우는 중"인가([TrafficReadProgress.isFilling]).
 * 같은 `(server, service, caller, trafficClass, filling)` 은 한 행이다.
 */
data class TrafficEdgeTotals(
    val server: String,
    val service: String,
    val caller: String,
    val trafficClass: TrafficClass,
    val filling: Boolean,
    val totals: TrafficTotals,
)

/** 시계열의 간격 하나의 합(서버 하나) — [lastCellEnd] 는 그 간격에 든 칸 가운데 가장 늦게 끝나는 칸의 끝("채우는 중" 판정용) */
data class TrafficStepTotals(val server: String, val stepStart: Instant, val lastCellEnd: Instant, val totals: TrafficTotals)

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
 *
 * 구현이 정한 표: `traffic_generation`(`(server, generation)` 마다의 순번 요약 — 처음 본 순번 · 가장 큰 순번 · 받은 수 · 틈 없이 이어진 끝과 그 `at` ·
 * 끝이 나아간 받은 시각 · 마지막 받은 시각) · `traffic_rollup_bucket` · `traffic_rollup` · `traffic_rollup_attempt`(자원 지표의 같은 이름 표와 같은 뜻).
 * 받은 열쇠 · 요약은 7일 뒤 지운다(요약은 마지막 받은 시각으로).
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
    private val transaction = TransactionTemplate(DataSourceTransactionManager(requireNotNull(ingest.dataSource) { "JdbcTemplate 에 DataSource 가 없다" }))
    @Volatile private var schemaReady = false
    private val creatingSchema = Any()

    /** 스키마 — 있으면 그대로 둔다(`IF NOT EXISTS` — 지표 저장소와 같은 방식과 한계) */
    override fun createSchema() = synchronized(creatingSchema) { createTables() }

    private fun createTables() {
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_receipt (
                 server            text   NOT NULL,
                 generation        text   NOT NULL,
                 seq               bigint NOT NULL,
                 read_ms           bigint NOT NULL,
                 received_ms       bigint NOT NULL,
                 requests_dropped  bigint NOT NULL,
                 requests_rejected bigint NOT NULL,
                 missing           text[] NOT NULL,
                 PRIMARY KEY (server, generation, seq))"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_receipt_received ON traffic_receipt (received_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_generation (
                 server             text   NOT NULL,
                 generation         text   NOT NULL,
                 first_seq          bigint NOT NULL,
                 max_seq            bigint NOT NULL,
                 received_count     bigint NOT NULL,
                 contiguous_seq     bigint NOT NULL,
                 contiguous_read_ms bigint,
                 advanced_ms        bigint,
                 first_received_ms  bigint NOT NULL,
                 last_received_ms   bigint NOT NULL,
                 PRIMARY KEY (server, generation))"""
        )
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_segment (
                 insert_order bigint   GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                 server       text     NOT NULL,
                 bucket_ms    bigint   NOT NULL,
                 service      text     NOT NULL,
                 caller       text     NOT NULL,
                 class        text     NOT NULL,
                 count        bigint   NOT NULL,
                 status_2xx   bigint   NOT NULL,
                 status_3xx   bigint   NOT NULL,
                 status_4xx   bigint   NOT NULL,
                 status_5xx   bigint   NOT NULL,
                 status_other bigint   NOT NULL,
                 buckets      bigint[] NOT NULL,
                 max_ms       bigint,
                 generation   text     NOT NULL,
                 seq          bigint   NOT NULL,
                 received_ms  bigint   NOT NULL)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_segment_server_bucket ON traffic_segment (server, bucket_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_segment_bucket ON traffic_segment (bucket_ms)")
        // 열쇠 없이 — 받은 열쇠가 7일 뒤 지워진 순번이 다시 오면 같은 (서버, 세대, 순번)의 줄이 또 들어올 수 있다(그 줄은 6일 검사가 대개 막는다)
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_request (
                 server          text    NOT NULL,
                 generation      text    NOT NULL,
                 seq             bigint  NOT NULL,
                 request_id      text    NOT NULL,
                 service         text    NOT NULL,
                 caller          text    NOT NULL,
                 method          text    NOT NULL,
                 path            text    NOT NULL,
                 path_truncated  boolean NOT NULL,
                 status          integer NOT NULL,
                 end_ms          bigint  NOT NULL,
                 duration_ms     bigint  NOT NULL,
                 upstream_ms     bigint,
                 upstream_status text,
                 slot            text,
                 bytes_sent      bigint  NOT NULL,
                 received_ms     bigint  NOT NULL)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_request_id ON traffic_request (request_id, end_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_request_end ON traffic_request (end_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_rollup_bucket (
                 server            text   NOT NULL,
                 bucket_ms         bigint NOT NULL,
                 computed_ms       bigint NOT NULL,
                 insert_order_seen bigint NOT NULL,
                 PRIMARY KEY (server, bucket_ms))"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_rollup_bucket_age ON traffic_rollup_bucket (bucket_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_rollup (
                 server       text     NOT NULL,
                 bucket_ms    bigint   NOT NULL,
                 service      text     NOT NULL,
                 caller       text     NOT NULL,
                 class        text     NOT NULL,
                 count        bigint   NOT NULL,
                 status_2xx   bigint   NOT NULL,
                 status_3xx   bigint   NOT NULL,
                 status_4xx   bigint   NOT NULL,
                 status_5xx   bigint   NOT NULL,
                 status_other bigint   NOT NULL,
                 buckets      bigint[] NOT NULL,
                 max_ms       bigint,
                 PRIMARY KEY (server, bucket_ms, service, caller, class),
                 FOREIGN KEY (server, bucket_ms) REFERENCES traffic_rollup_bucket ON DELETE CASCADE)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS traffic_rollup_by_bucket ON traffic_rollup (bucket_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS traffic_rollup_attempt (
                 server       text   NOT NULL,
                 bucket_ms    bigint NOT NULL,
                 attempted_ms bigint NOT NULL,
                 PRIMARY KEY (server, bucket_ms))"""
        )
        schemaReady = true
    }

    /** 조회 · 예약이 먼저 닿으면 그때 만든다. 헬스는 만들지 않는다 — 표 없음을 그대로 본다 */
    private fun ensureSchema() {
        if (!schemaReady) synchronized(creatingSchema) { if (!schemaReady) createTables() }
    }

    /** 뜬 직후 한 번 미리 만든다 — 실패는 예외 */
    fun prepareSchema() = ensureSchema()

    /**
     * 한 항목 = 한 트랜잭션. 표는 트랜잭션 **밖에서** 먼저 확인한다 — 트랜잭션의 첫 문장이 받은 열쇠의 `INSERT … ON CONFLICT DO NOTHING` 이게.
     * 같은 열쇠를 다른 연결이 넣는 중이면 그 첫 문장이 그 커밋까지 기다렸다가 0 행을 돌려준다(두 자리가 함께 칸을 넣지 않는다).
     */
    override fun store(entry: TrafficEntry): TrafficStoreOutcome {
        ensureSchema()
        return transaction.execute {
            val received = entry.receivedAt.toEpochMilli()
            val inserted = ingest.update(INSERT_RECEIPT, PreparedStatementSetter { statement ->
                statement.setString(1, entry.server); statement.setString(2, entry.generation); statement.setLong(3, entry.sequence)
                statement.setLong(4, entry.readAt.toEpochMilli()); statement.setLong(5, received)
                statement.setLong(6, entry.requestsDropped); statement.setLong(7, entry.requestsRejected)
                statement.setArray(8, statement.connection.createArrayOf("text", entry.missing.toTypedArray()))
            })
            if (inserted == 0) return@execute TrafficStoreOutcome(duplicate = true)
            advanceProgress(entry)
            val receivedAt = entry.receivedAt
            val segments = entry.segments.filter { !TrafficCellAge.isAhead(it.bucketStart, receivedAt) && !TrafficCellAge.isTooOld(it.bucketStart, receivedAt) }
            val requests = entry.requests.filter { !TrafficCellAge.isAhead(it.endAt, receivedAt) && !TrafficCellAge.isTooOld(it.endAt, receivedAt) }
            if (segments.isNotEmpty()) ingest.batchUpdate(INSERT_SEGMENT, segments, segments.size) { statement, segment ->
                val totals = segment.totals
                statement.setString(1, entry.server); statement.setLong(2, segment.bucketStart.toEpochMilli())
                statement.setString(3, segment.service); statement.setString(4, segment.caller); statement.setString(5, segment.trafficClass.wireName)
                statement.setLong(6, totals.count)
                statement.setLong(7, totals.status.status2xx); statement.setLong(8, totals.status.status3xx); statement.setLong(9, totals.status.status4xx)
                statement.setLong(10, totals.status.status5xx); statement.setLong(11, totals.status.other)
                statement.setArray(12, statement.connection.createArrayOf("int8", totals.buckets.toTypedArray()))
                statement.setNullable(13, totals.maxMs, Types.BIGINT)
                statement.setString(14, entry.generation); statement.setLong(15, entry.sequence); statement.setLong(16, received)
            }
            if (requests.isNotEmpty()) ingest.batchUpdate(INSERT_REQUEST, requests, requests.size) { statement, request ->
                statement.setString(1, entry.server); statement.setString(2, entry.generation); statement.setLong(3, entry.sequence)
                statement.setString(4, request.requestId); statement.setString(5, request.service); statement.setString(6, request.caller)
                statement.setString(7, request.method); statement.setString(8, request.path); statement.setBoolean(9, request.pathTruncated)
                statement.setInt(10, request.status); statement.setLong(11, request.endAt.toEpochMilli()); statement.setLong(12, request.durationMs)
                statement.setNullable(13, request.upstreamMs, Types.BIGINT); statement.setNullable(14, request.upstreamStatus, Types.VARCHAR)
                statement.setNullable(15, request.slot, Types.VARCHAR); statement.setLong(16, request.bytesSent); statement.setLong(17, received)
            }
            TrafficStoreOutcome(
                duplicate = false,
                segmentsStored = segments.size,
                segmentsAhead = entry.segments.count { TrafficCellAge.isAhead(it.bucketStart, receivedAt) },
                segmentsTooOld = entry.segments.count { TrafficCellAge.isTooOld(it.bucketStart, receivedAt) },
                requestsStored = requests.size,
                requestsAhead = entry.requests.count { TrafficCellAge.isAhead(it.endAt, receivedAt) },
                requestsTooOld = entry.requests.count { TrafficCellAge.isTooOld(it.endAt, receivedAt) },
            )
        }
    }

    /**
     * 순번 요약을 고치고 읽기 진도를 민다([TrafficServerProgress] 의 규칙) — 요약 행을 먼저 잠근다(upsert 가 그 행을 잡는다). 두 자리가 같은 세대의
     * 다른 순번을 함께 받으면 뒤의 것은 이 행에서 앞의 것의 커밋까지 기다렸다가, 앞의 것이 넣은 열쇠까지 보고 진도를 민다.
     * 처음 본 순번이 낮아졌으면 upsert 가 끝을 그 앞으로 되돌려 두었으므로 여기서 받은 열쇠를 따라 다시 잇는다(같은 행 잠금 안에서).
     */
    private fun advanceProgress(entry: TrafficEntry) {
        val received = entry.receivedAt.toEpochMilli()
        val summary = ingest.queryForMap(UPSERT_GENERATION, entry.server, entry.generation, entry.sequence, entry.sequence, entry.sequence - 1, received, received)
        val contiguous = (summary["contiguous_seq"] as Number).toLong()
        if (entry.sequence <= contiguous) return                      // 이미 지나간 순번이 늦게 왔다 — 틈의 수만 줄고 진도는 그대로
        val waiting = ingest.query(
            "SELECT seq, read_ms, received_ms FROM traffic_receipt WHERE server = ? AND generation = ? AND seq > ? ORDER BY seq",
            { row, _ -> Triple(row.getLong("seq"), row.getLong("read_ms"), row.getLong("received_ms")) },
            entry.server, entry.generation, contiguous,
        )
        // 틈 앞에서: 그 틈보다 뒤의 순번을 처음 받은 때 = 남은 열쇠들의 가장 이른 받은 시각
        val firstLaterReceived = LongArray(waiting.size)
        for (index in waiting.indices.reversed()) firstLaterReceived[index] = minOf(waiting[index].third, firstLaterReceived.getOrElse(index + 1) { Long.MAX_VALUE })
        var end = contiguous
        var readMax = (summary["contiguous_read_ms"] as Number?)?.toLong()
        for ((index, waitingEntry) in waiting.withIndex()) {
            val (sequence, readMs, _) = waitingEntry
            if (sequence != end + 1 && received - firstLaterReceived[index] <= gapWait.toMillis()) break   // 틈 — 아직 기다린다
            end = sequence
            readMax = maxOf(readMax ?: readMs, readMs)
        }
        if (end != contiguous) ingest.update(
            "UPDATE traffic_generation SET contiguous_seq = ?, contiguous_read_ms = ?, advanced_ms = ? WHERE server = ? AND generation = ?",
            end, readMax, received, entry.server, entry.generation,
        )
    }

    override fun cells(query: TrafficCellQuery): List<TrafficCell> {
        ensureSchema()
        val cells = ArrayList<TrafficCell>()
        val (filter, filterArguments) = filterOf(query)
        val from = query.from.toEpochMilli()
        val to = query.to.toEpochMilli()
        val rawSince = query.rawSince.toEpochMilli()
        if (from < rawSince) this.query.query(
            "SELECT server, bucket_ms, service, caller, class, $TOTALS_COLUMNS FROM traffic_rollup WHERE bucket_ms >= ? AND bucket_ms < ?$filter",
            RowCallbackHandler { row -> cells += row.toCell(ROLLUP) }, from, minOf(to, rawSince), *filterArguments,
        )
        if (to > rawSince) this.query.query(
            """SELECT server, bucket_ms, service, caller, class, $TOTALS_SUMMED FROM traffic_segment
               WHERE bucket_ms >= ? AND bucket_ms < ?$filter GROUP BY server, bucket_ms, service, caller, class""",
            RowCallbackHandler { row -> cells += row.toCell(CELL) }, maxOf(from, rawSince), to, *filterArguments,
        )
        return cells.sortedWith(compareBy({ it.server }, { it.bucketStart }, { it.service }, { it.caller }, { it.trafficClass.wireName }))
    }

    /**
     * 지도의 합을 SQL 에서 — 서버의 읽기 진도(세대들의 `contiguous_read_ms` 가운데 가장 큰 것)와 칸의 끝(`bucket_ms + 30초`)을 견줘 채우는 중을 가르고
     * (서버 · 서비스 · 호출자 · 갈래 · 채우는 중)으로 묶는다. 돌아오는 행은 선 × 서버 × 2 이하다.
     */
    override fun edgeTotals(from: Instant, to: Instant): List<TrafficEdgeTotals> {
        ensureSchema()
        val cell = CELL.toMillis()
        val margin = TrafficReadProgress.FILLING_MARGIN.toMillis()
        return query.query(
            """WITH progress AS (SELECT server, max(contiguous_read_ms) AS read_ms FROM traffic_generation GROUP BY server)
               SELECT segment.server, service, caller, class,
                      (progress.read_ms IS NULL OR segment.bucket_ms + ? > progress.read_ms - ?) AS filling, $TOTALS_SUMMED
               FROM traffic_segment segment LEFT JOIN progress ON progress.server = segment.server
               WHERE segment.bucket_ms >= ? AND segment.bucket_ms < ?
               GROUP BY segment.server, service, caller, class, filling""",
            { row, _ ->
                TrafficEdgeTotals(row.getString("server"), row.getString("service"), row.getString("caller"),
                    requireNotNull(TrafficClass.of(row.getString("class"))) { "저장된 갈래를 모른다" }, row.getBoolean("filling"), row.toTotals())
            },
            cell, margin, from.toEpochMilli(), to.toEpochMilli(),
        )
    }

    /** 칸을 간격으로 합치는 것까지 SQL 에서 — 원본(30초)과 5분 묶음을 이어 붙여 서버 · 간격마다 하나로 */
    override fun steps(query: TrafficCellQuery, origin: Instant, step: Duration): List<TrafficStepTotals> {
        ensureSchema()
        val (filter, filterArguments) = filterOf(query)
        val from = query.from.toEpochMilli()
        val to = query.to.toEpochMilli()
        val rawSince = query.rawSince.toEpochMilli()
        val parts = mutableListOf<String>()
        val arguments = mutableListOf<Any>(origin.toEpochMilli(), step.toMillis())
        if (from < rawSince) {
            parts += "SELECT server, bucket_ms, ${ROLLUP.toMillis()} AS width_ms, $TOTALS_COLUMNS FROM traffic_rollup WHERE bucket_ms >= ? AND bucket_ms < ?$filter"
            arguments.addAll(listOf(from, minOf(to, rawSince)) + filterArguments)
        }
        if (to > rawSince) {
            parts += "SELECT server, bucket_ms, ${CELL.toMillis()} AS width_ms, $TOTALS_COLUMNS FROM traffic_segment WHERE bucket_ms >= ? AND bucket_ms < ?$filter"
            arguments.addAll(listOf(maxOf(from, rawSince), to) + filterArguments)
        }
        if (parts.isEmpty()) return emptyList()
        return this.query.query(
            """SELECT server, (bucket_ms - ?) / ? AS step_index, max(bucket_ms + width_ms) AS last_end, $TOTALS_SUMMED
               FROM (${parts.joinToString(" UNION ALL ")}) cell GROUP BY server, step_index ORDER BY server, step_index""",
            { row, _ ->
                TrafficStepTotals(row.getString("server"), origin.plus(step.multipliedBy(row.getLong("step_index"))),
                    Instant.ofEpochMilli(row.getLong("last_end")), row.toTotals())
            },
            *arguments.toTypedArray(),
        )
    }

    /** 칸 조회의 거르개(서비스 · 호출자 · 갈래) — 값은 자리표시자로 */
    private fun filterOf(query: TrafficCellQuery): Pair<String, Array<Any>> {
        val conditions = listOfNotNull(query.service?.let { "service" to it }, query.caller?.let { "caller" to it }, query.trafficClass?.let { "class" to it.wireName })
        return conditions.joinToString("") { " AND ${it.first} = ?" } to conditions.map { it.second }.toTypedArray()
    }

    override fun serverProgress(): List<TrafficServerProgress> {
        ensureSchema()
        return progress(query)
    }

    /** 헬스용 — 헬스의 선택 칸 연결로 읽고 표를 만들지 않는다 */
    fun healthProgress(): List<TrafficServerProgress> = progress(healthDetail)

    private fun progress(jdbc: JdbcTemplate): List<TrafficServerProgress> {
        data class Generation(val server: String, val firstSeq: Long, val gaps: Long, val readMs: Long?, val advancedMs: Long?,
                              val firstReceivedMs: Long, val lastReceivedMs: Long)
        val generations = jdbc.query(
            """SELECT server, first_seq, GREATEST(0, max_seq - first_seq + 1 - received_count) AS gaps, contiguous_read_ms, advanced_ms,
                      first_received_ms, last_received_ms
               FROM traffic_generation""",
            { row, _ ->
                Generation(row.getString("server"), row.getLong("first_seq"), row.getLong("gaps"), row.nullableLong("contiguous_read_ms"),
                    row.nullableLong("advanced_ms"), row.getLong("first_received_ms"), row.getLong("last_received_ms"))
            },
        )
        return generations.groupBy { it.server }.toSortedMap().map { (server, list) ->
            val latest = list.maxWith(compareBy({ it.lastReceivedMs }, { it.firstReceivedMs }))
            TrafficServerProgress(
                server = server,
                readProgressAt = list.mapNotNull { it.readMs }.maxOrNull()?.let(Instant::ofEpochMilli),
                progressAdvancedAt = list.mapNotNull { it.advancedMs }.maxOrNull()?.let(Instant::ofEpochMilli),
                sequenceGaps = list.sumOf { it.gaps },
                lastReceivedAt = Instant.ofEpochMilli(list.maxOf { it.lastReceivedMs }),
                firstSeqSeen = latest.firstSeq,
            )
        }
    }

    override fun entrySummaries(from: Instant, to: Instant): Map<String, TrafficEntrySummary> {
        ensureSchema()
        val missing = query.query(
            """SELECT DISTINCT server, missing_item FROM traffic_receipt, unnest(missing) missing_item
               WHERE received_ms >= ? AND received_ms < ? ORDER BY server, missing_item""",
            { row, _ -> row.getString("server") to row.getString("missing_item") }, from.toEpochMilli(), to.toEpochMilli(),
        ).groupBy({ it.first }, { it.second })
        return query.query(
            """SELECT server, sum(requests_dropped)::bigint AS dropped, sum(requests_rejected)::bigint AS rejected FROM traffic_receipt
               WHERE received_ms >= ? AND received_ms < ? GROUP BY server ORDER BY server""",
            { row, _ -> row.getString("server") to TrafficEntrySummary(missing[row.getString("server")].orEmpty(), row.getLong("dropped"), row.getLong("rejected")) },
            from.toEpochMilli(), to.toEpochMilli(),
        ).toMap()
    }

    override fun requestSpans(requestId: String, limit: Int): List<TrafficSpan> {
        ensureSchema()
        return query.query(
            "SELECT * FROM traffic_request WHERE request_id = ? ORDER BY end_ms, service LIMIT ?",
            { row, _ ->
                TrafficSpan(row.getString("server"), TrafficRequest(
                    requestId = row.getString("request_id"), service = row.getString("service"), caller = row.getString("caller"),
                    method = row.getString("method"), path = row.getString("path"), pathTruncated = row.getBoolean("path_truncated"),
                    status = row.getInt("status"), endAt = Instant.ofEpochMilli(row.getLong("end_ms")), durationMs = row.getLong("duration_ms"),
                    upstreamMs = row.nullableLong("upstream_ms"), upstreamStatus = row.getString("upstream_status"), slot = row.getString("slot"),
                    bytesSent = row.getLong("bytes_sent"),
                ))
            },
            requestId, limit,
        )
    }

    /** 가장 최근에 받은 항목의 받은 시각(헬스용) — 없으면 null, 표가 아직 없으면 SQLSTATE 42P01 예외 */
    fun latestStoredAt(): Instant? =
        health.queryForObject("SELECT max(last_received_ms) FROM traffic_generation", Long::class.javaObjectType)?.let(Instant::ofEpochMilli)

    /** 끝난 5분 구간을 모두 다시 계산할 만큼 돈다(개수 제한 없이). 예약 실행은 [rollUpBuckets] 를 쓴다 */
    override fun rollUp(now: Instant): Int = rollUpBuckets(now, limit = null).computed

    override fun rollUpBuckets(now: Instant): RollupOutcome = rollUpBuckets(now, BUCKETS_PER_ROUND)

    /** 다시 계산할 (서버, 5분 구간)을 고르는 규칙 · 차례 · 구간마다 따로 한 트랜잭션 — [xyz.junproject.observe.metric.MetricStore] 의 5분 평균과 같다 */
    private fun rollUpBuckets(now: Instant, limit: Int?): RollupOutcome {
        ensureSchema()
        val bucket = ROLLUP.toMillis()
        val targets = ingest.query(
            """SELECT raw.server, raw.bucket_ms FROM (
                 SELECT server, bucket_ms / ? * ? AS bucket_ms, max(received_ms) AS last_received, max(insert_order) AS last_order
                 FROM traffic_segment WHERE bucket_ms < ? GROUP BY 1, 2) raw
               LEFT JOIN traffic_rollup_bucket done ON done.server = raw.server AND done.bucket_ms = raw.bucket_ms
               LEFT JOIN traffic_rollup_attempt tried ON tried.server = raw.server AND tried.bucket_ms = raw.bucket_ms
               WHERE done.server IS NULL OR done.computed_ms <= raw.last_received OR done.insert_order_seen < raw.last_order
               ORDER BY tried.attempted_ms NULLS FIRST, raw.server, raw.bucket_ms
               LIMIT ?""",
            { row, _ -> row.getString("server") to row.getLong("bucket_ms") },
            bucket, bucket, SeriesPlan.floorTo(now, ROLLUP).toEpochMilli(), limit ?: Int.MAX_VALUE,
        )
        var computed = 0
        var failed = 0
        var firstFailure: String? = null
        for ((server, bucketStart) in targets) {
            try {
                rollUpOne(server, bucketStart, now)
                computed++
            } catch (error: Exception) {
                failed++
                if (firstFailure == null) firstFailure = error.javaClass.simpleName
            }
            runCatching {
                ingest.update(
                    """INSERT INTO traffic_rollup_attempt (server, bucket_ms, attempted_ms) VALUES (?,?,?)
                       ON CONFLICT (server, bucket_ms) DO UPDATE SET attempted_ms = EXCLUDED.attempted_ms""",
                    server, bucketStart, now.toEpochMilli(),
                )
            }
        }
        if (failed > 0) log.warn("observe:observe:traffic rollup failed for {} buckets (first: {})", failed, firstFailure)
        return RollupOutcome(computed, failed, firstFailure)
    }

    /** (서버, 5분 구간) 하나를 한 트랜잭션에서 원본의 합으로 덮어쓰고 "계산함" 표시를 함께 확정한다 — 잠금과 저장 순번의 까닭은 지표의 같은 함수 */
    private fun rollUpOne(server: String, bucketStart: Long, now: Instant) {
        transaction.executeWithoutResult {
            ingest.execute("LOCK TABLE traffic_segment IN SHARE MODE")
            val seen = lastInsertOrder(ingest, "traffic_segment")
            ingest.update(
                """INSERT INTO traffic_rollup_bucket (server, bucket_ms, computed_ms, insert_order_seen) VALUES (?,?,?,?)
                   ON CONFLICT (server, bucket_ms) DO UPDATE SET computed_ms = EXCLUDED.computed_ms, insert_order_seen = EXCLUDED.insert_order_seen""",
                server, bucketStart, now.toEpochMilli(), seen,
            )
            ingest.update("DELETE FROM traffic_rollup WHERE server = ? AND bucket_ms = ?", server, bucketStart)
            ingest.update(
                """INSERT INTO traffic_rollup (server, bucket_ms, service, caller, class, $TOTALS_COLUMNS)
                   SELECT server, ?, service, caller, class, $TOTALS_SUMMED FROM traffic_segment
                   WHERE server = ? AND bucket_ms >= ? AND bucket_ms < ? GROUP BY server, service, caller, class""",
                bucketStart, server, bucketStart, bucketStart + ROLLUP.toMillis(),
            )
        }
    }

    /**
     * 원본 칸: 구간 전체가 `now − 7일` 앞에 있고 5분 묶음이 그 구간의 원본을 모두 담은 (서버, 5분 구간)만 — 지표의 원본 삭제와 같은 조건.
     * 요청별 줄은 `end_ms`, 받은 열쇠는 `received_ms`, 순번 요약은 마지막 받은 시각이 `now − 7일` 앞이면. 5분 묶음은 구간 시작이 `now − 30일` 앞이면.
     */
    override fun deleteExpired(now: Instant): TrafficDeletion {
        ensureSchema()
        val rawCutoff = now.minus(RAW_RETENTION).toEpochMilli()
        val bucket = ROLLUP.toMillis()
        val segmentsDeleted = ingest.update(
            """DELETE FROM traffic_segment segment
               USING (
                 SELECT done.server, done.bucket_ms FROM traffic_rollup_bucket done
                 JOIN (SELECT server, bucket_ms / ? * ? AS bucket_ms, max(received_ms) AS last_received, max(insert_order) AS last_order
                       FROM traffic_segment WHERE bucket_ms < ? GROUP BY 1, 2) raw
                   ON raw.server = done.server AND raw.bucket_ms = done.bucket_ms
                 WHERE done.computed_ms > raw.last_received AND done.insert_order_seen >= raw.last_order
               ) rolled
               WHERE segment.server = rolled.server
                 AND segment.bucket_ms >= rolled.bucket_ms AND segment.bucket_ms < rolled.bucket_ms + ?
                 AND segment.bucket_ms < ?""",
            bucket, bucket, SeriesPlan.floorTo(Instant.ofEpochMilli(rawCutoff), ROLLUP).toEpochMilli(), bucket, rawCutoff,
        )
        val requestsDeleted = ingest.update("DELETE FROM traffic_request WHERE end_ms < ?", rawCutoff)
        val entriesDeleted = ingest.update("DELETE FROM traffic_receipt WHERE received_ms < ?", rawCutoff)
        ingest.update("DELETE FROM traffic_generation WHERE last_received_ms < ?", rawCutoff)
        val rollupCutoff = now.minus(ROLLUP_RETENTION).toEpochMilli()
        val rollupDeleted = ingest.update("DELETE FROM traffic_rollup_bucket WHERE bucket_ms < ?", rollupCutoff)
        ingest.update("DELETE FROM traffic_rollup_attempt WHERE bucket_ms < ?", rollupCutoff)
        return TrafficDeletion(entriesDeleted, segmentsDeleted, requestsDeleted, rollupDeleted)
    }

    private fun ResultSet.toTotals() = TrafficTotals(
        count = getLong("count"),
        status = TrafficStatusCounts(getLong("status_2xx"), getLong("status_3xx"), getLong("status_4xx"), getLong("status_5xx"), getLong("status_other")),
        buckets = (getArray("buckets").array as Array<*>).map { (it as Number).toLong() },
        maxMs = nullableLong("max_ms"),
    )

    private fun ResultSet.toCell(width: Duration) = TrafficCell(
        getString("server"), Instant.ofEpochMilli(getLong("bucket_ms")), width, getString("service"), getString("caller"),
        requireNotNull(TrafficClass.of(getString("class"))) { "저장된 갈래를 모른다" }, toTotals(),
    )

    companion object {
        val RAW_RETENTION: Duration = Duration.ofDays(7)
        val ROLLUP_RETENTION: Duration = Duration.ofDays(30)
        val ROLLUP: Duration = Duration.ofMinutes(5)
        val CELL: Duration = Duration.ofSeconds(30)
        const val BUCKETS_PER_ROUND = 200
        val DEFAULT_GAP_WAIT: Duration = Duration.ofMinutes(10)
        private val log = LoggerFactory.getLogger("observe")

        /** 칸의 값 칸들 — 원본 행 · 5분 묶음이 같은 이름 */
        private const val TOTALS_COLUMNS = "count, status_2xx, status_3xx, status_4xx, status_5xx, status_other, buckets, max_ms"

        /** 칸의 값의 합 — 수 · 상태 · 분포는 칸마다 더하고 `max_ms` 는 큰 쪽(규약 4.2). 분포는 20칸을 자리마다 더한다 */
        private val TOTALS_SUMMED =
            "sum(count)::bigint AS count, " + listOf("2xx", "3xx", "4xx", "5xx", "other").joinToString { "sum(status_$it)::bigint AS status_$it" } +
                ", ARRAY[" + (1..TrafficDistribution.BUCKET_COUNT).joinToString { "sum(buckets[$it])::bigint" } + "] AS buckets, max(max_ms) AS max_ms"

        /** 트랜잭션의 첫 문장 — 넣은 행이 0 이면 이미 받은 순번(규약 4.5) */
        private const val INSERT_RECEIPT =
            """INSERT INTO traffic_receipt (server, generation, seq, read_ms, received_ms, requests_dropped, requests_rejected, missing)
               VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (server, generation, seq) DO NOTHING"""

        /**
         * 순번 요약 — 처음 본 순번은 그 세대에서 받은 가장 작은 순번이고(먼저 커밋된 순번이 아니다) 받은 수는 늘 센다. 처음 본 순번이 낮아지면
         * 끝을 그 앞으로 되돌리고 끝의 `at` 을 비운다 — 받은 열쇠를 따라 다시 잇는다([advanceProgress]). SET 의 `summary.*` 는 고치기 전의 값이다
         */
        private const val UPSERT_GENERATION =
            """INSERT INTO traffic_generation AS summary (server, generation, first_seq, max_seq, received_count, contiguous_seq, first_received_ms, last_received_ms)
               VALUES (?, ?, ?, ?, 1, ?, ?, ?)
               ON CONFLICT (server, generation) DO UPDATE SET
                 first_seq = LEAST(summary.first_seq, EXCLUDED.first_seq),
                 max_seq = GREATEST(summary.max_seq, EXCLUDED.max_seq),
                 received_count = summary.received_count + 1,
                 contiguous_seq = CASE WHEN EXCLUDED.first_seq < summary.first_seq THEN EXCLUDED.contiguous_seq ELSE summary.contiguous_seq END,
                 contiguous_read_ms = CASE WHEN EXCLUDED.first_seq < summary.first_seq THEN NULL ELSE summary.contiguous_read_ms END,
                 last_received_ms = GREATEST(summary.last_received_ms, EXCLUDED.last_received_ms)
               RETURNING contiguous_seq, contiguous_read_ms"""

        private const val INSERT_SEGMENT =
            """INSERT INTO traffic_segment (server, bucket_ms, service, caller, class, count, status_2xx, status_3xx, status_4xx, status_5xx, status_other,
                 buckets, max_ms, generation, seq, received_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        private const val INSERT_REQUEST =
            """INSERT INTO traffic_request (server, generation, seq, request_id, service, caller, method, path, path_truncated, status, end_ms,
                 duration_ms, upstream_ms, upstream_status, slot, bytes_sent, received_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        private fun PreparedStatement.setNullable(index: Int, value: Any?, sqlType: Int) =
            if (value == null) setNull(index, sqlType) else setObject(index, value, sqlType)

        private fun ResultSet.nullableLong(column: String): Long? = getLong(column).takeUnless { wasNull() }
    }
}
