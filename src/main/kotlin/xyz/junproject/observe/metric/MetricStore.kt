package xyz.junproject.observe.metric

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowCallbackHandler
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import xyz.junproject.observe.log.StreamId
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Duration
import java.time.Instant

/** 수집 쪽이 쓰는 면 */
interface MetricSink {
    /** 표를 만든다(있으면 그대로) — 추가만, 기존 `log_entry` · `auth_*` 는 건드리지 않는다. 실패는 예외. */
    fun createSchema()
    /** 묶음을 저장한다. 이미 있는 항목(같은 스트림 ID)은 건너뛴다. 새로 들어간 항목 수를 돌려준다. 실패는 예외(묶음 전체가 들어가지 않는다). */
    fun insertAll(samples: List<ResourceSample>): Int
}

/**
 * 계열 조회 조건 — 값 검증과 간격 선택([SeriesPlan])은 API 경계에서 끝낸다.
 * [name]: host 는 null, container 는 계열 이름(자리 이름을 뗀 것), gpu 는 번호 글자(`"0"`).
 * 기간은 `[from, to)`, 점은 `from + i × step` 칸. 측정 시각이 [rawSince] 보다 앞선 부분은 5분 평균에서 읽는다.
 */
data class SeriesQuery(
    val server: String,
    val scope: MetricScope,
    val name: String?,
    val from: Instant,
    val to: Instant,
    val step: Duration,
    val rawSince: Instant,
)

/**
 * 점 하나 — [at] 은 그 칸의 시작(`from + i × step`). [values] 는 값 이름 → 묶음이고 표본이 있는 값만 든다.
 * 표본이 하나도 없는 칸은 점이 없다(0 으로 채우지 않는다).
 * 컨테이너는 같은 항목(같은 시각) 안의 두 자리 값을 먼저 더한 것이 표본 하나다.
 */
data class SeriesPoint(val at: Instant, val values: Map<String, MetricAggregate>)

/** 5분 평균 하나 — [bucketStart] 는 정각 5분 경계(UTC epoch 기준). 컨테이너는 계열 이름으로 묶는다(두 자리의 합이 표본 하나) */
data class MetricRollup(val bucketStart: Instant, val values: Map<String, MetricAggregate>)

/** 한 서버에서 보인 컨테이너(계열 이름)와 기간 안에서 마지막으로 보인 측정 시각 */
data class SeenContainer(val name: String, val lastSeenAt: Instant)

/** 조회 쪽이 쓰는 면 */
interface MetricReader {
    fun series(query: SeriesQuery): List<SeriesPoint>

    /** 저장된 5분 평균(계열 하나, `from ≤ bucketStart < to`, 시각순) */
    fun rollups(server: String, scope: MetricScope, name: String?, from: Instant, to: Instant): List<MetricRollup>

    /** 서버마다 측정 시각이 가장 늦은 항목 하나 — 수신 시각이 [receivedSince] 이후인 항목이 있는 서버만. 서버 이름순 */
    fun latest(receivedSince: Instant): List<ResourceSample>

    /** [server] 에서 측정 시각이 `[from, to]`(양 끝 포함) 안인 항목에 들어 있던 컨테이너(계열 이름) — 이름순 */
    fun containers(server: String, from: Instant, to: Instant): List<SeenContainer>

    /** 서버별 마지막 수신 시각(원본과 따로 남긴 값 — §15 · §16). `latest` 의 서버 목록 · `age_ms` · `stale` 이 쓴다. 모르면 빈 지도 */
    fun serverReceipts(): Map<String, Instant> = emptyMap()
}

/** 원본 7일 · 5분 평균 30일 삭제 한 번의 결과 — [rawDeleted] 는 지운 항목(스트림 항목) 수, [rollupDeleted] 는 지운 (서버, 5분 구간) 수 */
data class MetricDeletion(val rawDeleted: Int, val rollupDeleted: Int)

/** 지표 삭제 한 번의 결과(헬스용) — 실패면 [deletion] 은 null 이고 [failure] 에 예외 종류 이름 */
data class MetricRetentionResult(val ranAt: Instant, val deletion: MetricDeletion?, val failure: String?)

/** 5분 평균 한 바퀴의 결과(헬스용) — 실패면 [buckets] 는 null 이고 [failure] 에 예외 종류 이름 */
data class MetricRollupResult(val ranAt: Instant, val buckets: Int?, val failure: String?, val failedBuckets: Int = 0)

/** 5분 평균 한 바퀴의 구간 수(§16) — [computed] 는 확정한 (서버, 구간), [failed] 는 계산에 실패해 표시 없이 남긴 (서버, 구간) */
data class RollupOutcome(
    val computed: Int,
    val failed: Int,
    /** §16.2: 그 바퀴의 첫 실패의 예외 종류 이름(실패가 없으면 null) — 헬스의 `last_rollup_failure` 에 실패 구간 수와 함께 남는다 */
    val firstFailure: String? = null,
)

/** 예약 실행이 쓰는 면 — 5분 평균과 보관 삭제. 시각은 늘 인자로 받는다(테스트가 시계를 정한다) */
interface MetricMaintenance {
    /**
     * 끝난 5분 구간(`bucketStart + 5분 ≤ now`) 중 **평균이 없거나, 평균의 계산 시각이 그 구간 원본의 마지막 수신 시각보다 뒤가 아닌**
     * (서버, 구간)을 원본에서 다시 계산해 덮어쓴다. 계산 시각은 [now]. 여러 번 돌려도 결과가 같다. 계산한 (서버, 구간) 수를 돌려준다.
     * 원본 값이 모두 null 인 구간도 "계산함"으로 남긴다(그래야 그 원본을 지울 수 있다).
     */
    fun rollUp(now: Instant): Int

    /**
     * §16 · §16.1: [rollUp] 과 같은 일을 **구간마다 따로** 한다 — 한 (서버, 구간)의 계산 실패가 다른 구간의 결과를 되돌리지 않고,
     * 그 구간의 평균 값과 "계산함" 표시는 같은 트랜잭션에서 확정된다(실패한 구간에는 표시가 없다). 한 바퀴는 가장 오래 시도하지 않은 구간부터
     * 200 구간까지. 예외는 구간 밖의 실패(DB 에 닿지 못함 등)만 낸다. 기본 동작은 [rollUp] 을 그대로 부르는 것(가짜 저장소용).
     */
    fun rollUpBuckets(now: Instant): RollupOutcome = RollupOutcome(rollUp(now), 0)

    /**
     * 원본: 측정 시각이 `now − 7일` 보다 앞서고, **그 (서버, 5분 구간)의 평균 계산 시각이 그 구간 원본의 마지막 수신 시각보다 뒤인** 것만 지운다
     * (평균이 없거나 평균보다 늦게 들어온 원본이 있는 구간은 하나도 지우지 않는다).
     * 5분 평균: 구간 시작이 `now − 30일` 보다 앞선 것을 지운다. 실패는 예외.
     */
    fun deleteExpired(now: Instant): MetricDeletion
}

/**
 * PostgreSQL 저장소 — 로그와 같은 DB, 표는 추가만(`log_entry` · `auth_*` 는 건드리지 않는다). 수집·조회·헬스의 연결 풀을 나눈다.
 * 표 이름과 꼴은 구현이 정한다(테스트는 이 면으로만 본다). 같은 스트림 ID 의 항목은 한 번만 남는다.
 *
 * 표(시각은 모두 epoch 밀리초 — 5분 구간 = `measured_ms / 300000 * 300000`):
 * - `metric_sample`: 스트림 항목 하나 = 한 행(기본 키 = 스트림 ID). 서버 전체 값 · `missing` · 측정/수신 시각.
 *   `insert_order` 는 DB 가 매기는 저장 순번 — 5분 평균이 "그 구간의 원본을 모두 담았는가"를 시계와 무관하게 판정한다(아래 [rollUp]).
 * - `metric_container` · `metric_gpu`: 항목 안의 컨테이너 · GPU 하나 = 한 행(항목이 지워지면 함께 지워진다).
 *   조회 꼴(서버 · 계열 이름 또는 번호 · 측정 시각 범위)에 맞춘 인덱스.
 * - `metric_rollup_bucket`: (서버, 5분 구간)의 평균 계산 기록 — 계산 시각과 그때 본 마지막 저장 순번.
 * - `metric_rollup`: (서버, 구간, 갈래, 계열 이름, 값 이름)마다 평균 · 최댓값 · 표본 수. host 의 계열 이름은 빈 글자.
 * - `metric_rollup_attempt`: (서버, 5분 구간)을 마지막으로 계산해 보려 한 시각 — 성패와 상관없이. "계산함" 표시(`metric_rollup_bucket`)와 따로 두어
 *   실패한 구간에는 표시가 없고 차례만 뒤로 간다.
 * - `metric_server_receipt`: 서버별 마지막 수신 시각 — 새로 저장된 항목만, 큰 쪽으로(원본이 지워져도 남는다).
 */
class MetricStore(
    private val ingest: JdbcTemplate,
    private val query: JdbcTemplate,
    private val health: JdbcTemplate,
    /**
     * §16: 임계값 판정이 읽는 연결([samplesMeasuredBetween] · [lastReceivedOfEveryServer]) — 어드민 조회가
     * [query] 를 다 잡아도 판정이 기다리지 않게. 없으면 [query].
     */
    private val judge: JdbcTemplate = query,
    /**
     * 헬스의 선택 칸(서버별 수신 시각 · 최근 항목)을 읽는 연결 — 상태를 정하는 읽기([latestStoredAt] — [health])와 풀을 나눠, 선택 칸의 표가
     * 막혀 버려진 읽기가 연결을 쥐고 있어도 상태 읽기는 기다리지 않는다(§16.1). 없으면 [health].
     */
    private val healthDetail: JdbcTemplate = health,
) : MetricSink, MetricReader, MetricMaintenance, MetricSampleSource {
    private val transaction = TransactionTemplate(DataSourceTransactionManager(requireNotNull(ingest.dataSource) { "JdbcTemplate 에 DataSource 가 없다" }))
    @Volatile private var schemaReady = false
    private val creatingSchema = Any()

    /**
     * 스키마 — 있으면 그대로 둔다(마이그레이션 도구 없이 `IF NOT EXISTS`). 두 자리가 동시에 처음 만들면 한쪽이 고유 제약 위반으로
     * 실패할 수 있다(`log_entry` 와 같은 한계) — 그 호출이 실패로 남고 다음 호출이 다시 만든다.
     */
    override fun createSchema() = synchronized(creatingSchema) { createTables() }

    /** 표 만들기 — 이 프로세스 안에서는 [creatingSchema] 로 한 줄로 선다(수집 스레드와 뜬 직후 미리 만들기가 겹쳐 한쪽이 실패하지 않게 · §16) */
    private fun createTables() {
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_sample (
                 stream_ms    bigint           NOT NULL,
                 stream_seq   integer          NOT NULL,
                 server       text             NOT NULL,
                 measured_ms  bigint           NOT NULL,
                 received_ms  bigint           NOT NULL,
                 insert_order bigint           GENERATED ALWAYS AS IDENTITY,
                 interval_ms  bigint,
                 host_present boolean          NOT NULL,
                 cpu_pct      double precision,
                 load1        double precision,
                 mem_total    bigint,
                 mem_used     bigint,
                 disk_total   bigint,
                 disk_used    bigint,
                 net_rx_bps   double precision,
                 net_tx_bps   double precision,
                 io_read_bps  double precision,
                 io_write_bps double precision,
                 missing      text[]           NOT NULL,
                 PRIMARY KEY (stream_ms, stream_seq))"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_sample_server_measured ON metric_sample (server, measured_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_sample_server_received ON metric_sample (server, received_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_sample_received ON metric_sample (received_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_container (
                 stream_ms    bigint           NOT NULL,
                 stream_seq   integer          NOT NULL,
                 position     integer          NOT NULL,
                 server       text             NOT NULL,
                 measured_ms  bigint           NOT NULL,
                 name         text             NOT NULL,
                 series_name  text             NOT NULL,
                 container_id text             NOT NULL,
                 project      text             NOT NULL,
                 cpu_pct      double precision,
                 mem_used     bigint,
                 restarts     bigint,
                 state        text,
                 PRIMARY KEY (stream_ms, stream_seq, position),
                 FOREIGN KEY (stream_ms, stream_seq) REFERENCES metric_sample ON DELETE CASCADE)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_container_series ON metric_container (server, series_name, measured_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_container_seen ON metric_container (server, measured_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_container_measured ON metric_container (measured_ms)")   // 판정 입력(서버 섞임 · 측정 시각 범위)
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_gpu (
                 stream_ms   bigint           NOT NULL,
                 stream_seq  integer          NOT NULL,
                 position    integer          NOT NULL,
                 server      text             NOT NULL,
                 measured_ms bigint           NOT NULL,
                 gpu_index   integer          NOT NULL,
                 util_pct    double precision,
                 mem_used    bigint,
                 mem_total   bigint,
                 temp_c      double precision,
                 PRIMARY KEY (stream_ms, stream_seq, position),
                 FOREIGN KEY (stream_ms, stream_seq) REFERENCES metric_sample ON DELETE CASCADE)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_gpu_series ON metric_gpu (server, measured_ms)")
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_gpu_measured ON metric_gpu (measured_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_rollup_bucket (
                 server            text   NOT NULL,
                 bucket_ms         bigint NOT NULL,
                 computed_ms       bigint NOT NULL,
                 insert_order_seen bigint NOT NULL,
                 PRIMARY KEY (server, bucket_ms))"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_rollup_bucket_age ON metric_rollup_bucket (bucket_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_rollup (
                 server     text             NOT NULL,
                 bucket_ms  bigint           NOT NULL,
                 scope      text             NOT NULL,
                 name       text             NOT NULL,
                 value_name text             NOT NULL,
                 average    double precision NOT NULL,
                 maximum    double precision NOT NULL,
                 samples    bigint           NOT NULL,
                 PRIMARY KEY (server, scope, name, bucket_ms, value_name),
                 FOREIGN KEY (server, bucket_ms) REFERENCES metric_rollup_bucket ON DELETE CASCADE)"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS metric_rollup_by_bucket ON metric_rollup (server, bucket_ms)")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_server_receipt (
                 server           text   PRIMARY KEY,
                 last_received_ms bigint NOT NULL)"""
        )
        // 처음 채우기 — 이 표가 생기기 전부터 원본이 있던 DB 에서 비어 있지 않게. 큰 쪽으로만 바꾸므로 몇 번을, 두 자리가 함께 해도 결과가 같다
        ingest.update(
            """INSERT INTO metric_server_receipt (server, last_received_ms)
               SELECT server, max(received_ms) FROM metric_sample GROUP BY server ORDER BY server
               ON CONFLICT (server) DO UPDATE SET last_received_ms = GREATEST(metric_server_receipt.last_received_ms, EXCLUDED.last_received_ms)"""
        )
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS metric_rollup_attempt (
                 server       text   NOT NULL,
                 bucket_ms    bigint NOT NULL,
                 attempted_ms bigint NOT NULL,
                 PRIMARY KEY (server, bucket_ms))"""
        )
        schemaReady = true
    }

    /** 조회·예약이 먼저 닿으면 그때 만든다(수집 바퀴보다 먼저 불려도 표 없음으로 실패하지 않게). 헬스는 만들지 않는다 — 표 없음을 그대로 본다 */
    private fun ensureSchema() {
        if (!schemaReady) synchronized(creatingSchema) { if (!schemaReady) createTables() }
    }

    /** 뜬 직후 한 번 미리 만든다 — 실패는 예외로(부르는 쪽이 로그만 남기고 삼킨다) */
    fun prepareSchema() = ensureSchema()

    override fun insertAll(samples: List<ResourceSample>): Int {
        if (samples.isEmpty()) return 0
        return transaction.execute {
            val results = ingest.batchUpdate(INSERT_SAMPLE, samples, samples.size) { statement, sample -> bindSample(statement, sample) }
                .flatMap { it.asList() }
            // 새로 들어간 항목의 컨테이너 · GPU 만 넣는다 — 이미 있던 항목은 처음 받은 그대로 둔다
            val inserted = samples.filterIndexed { index, _ -> results[index] > 0 || results[index] == java.sql.Statement.SUCCESS_NO_INFO }
            val containers = inserted.flatMap { sample -> sample.containers.mapIndexed { position, container -> Triple(sample, position, container) } }
            if (containers.isNotEmpty()) ingest.batchUpdate(INSERT_CONTAINER, containers, containers.size) { statement, (sample, position, container) ->
                statement.setLong(1, sample.id.millis); statement.setInt(2, sample.id.sequence); statement.setInt(3, position)
                statement.setString(4, sample.server); statement.setLong(5, sample.measuredAt.toEpochMilli())
                statement.setString(6, container.name); statement.setString(7, ContainerName.seriesName(container.name))
                statement.setString(8, container.id); statement.setString(9, container.project)
                statement.setNullable(10, container.cpuPct, Types.DOUBLE); statement.setNullable(11, container.memUsed, Types.BIGINT)
                statement.setNullable(12, container.restarts, Types.BIGINT); statement.setNullable(13, container.state, Types.VARCHAR)
            }
            val gpus = inserted.flatMap { sample -> sample.gpus.mapIndexed { position, gpu -> Triple(sample, position, gpu) } }
            if (gpus.isNotEmpty()) ingest.batchUpdate(INSERT_GPU, gpus, gpus.size) { statement, (sample, position, gpu) ->
                statement.setLong(1, sample.id.millis); statement.setInt(2, sample.id.sequence); statement.setInt(3, position)
                statement.setString(4, sample.server); statement.setLong(5, sample.measuredAt.toEpochMilli()); statement.setInt(6, gpu.index)
                statement.setNullable(7, gpu.utilPct, Types.DOUBLE); statement.setNullable(8, gpu.memUsed, Types.BIGINT)
                statement.setNullable(9, gpu.memTotal, Types.BIGINT); statement.setNullable(10, gpu.tempC, Types.DOUBLE)
            }
            // 서버별 마지막 수신 시각 — 같은 트랜잭션에서, 큰 쪽으로만(두 자리가 엇갈려 커밋해도 옛 값이 새 값을 덮지 않는다).
            // 서버 이름순으로 잡아 두 저장이 서로의 줄을 거꾸로 기다리지 않게
            // 다시 전달된 항목(이미 있던 스트림 ID)은 수신으로 치지 않는다 — 실제로 새로 들어간 항목의 서버만(§16)
            val receipts = inserted.groupBy { it.server }.mapValues { (_, list) -> list.maxOf { it.receivedAt.toEpochMilli() } }.toSortedMap()
            if (receipts.isNotEmpty()) ingest.batchUpdate(UPSERT_RECEIPT, receipts.entries.toList(), receipts.size) { statement, (server, receivedMs) ->
                statement.setString(1, server); statement.setLong(2, receivedMs)
            }
            inserted.size
        }!!
    }

    private fun bindSample(statement: PreparedStatement, sample: ResourceSample) {
        val host = sample.host
        statement.setLong(1, sample.id.millis); statement.setInt(2, sample.id.sequence); statement.setString(3, sample.server)
        statement.setLong(4, sample.measuredAt.toEpochMilli()); statement.setLong(5, sample.receivedAt.toEpochMilli())
        statement.setNullable(6, sample.intervalMs, Types.BIGINT); statement.setBoolean(7, host != null)
        statement.setNullable(8, host?.cpuPct, Types.DOUBLE); statement.setNullable(9, host?.load1, Types.DOUBLE)
        statement.setNullable(10, host?.memTotal, Types.BIGINT); statement.setNullable(11, host?.memUsed, Types.BIGINT)
        statement.setNullable(12, host?.diskTotal, Types.BIGINT); statement.setNullable(13, host?.diskUsed, Types.BIGINT)
        statement.setNullable(14, host?.netRxBps, Types.DOUBLE); statement.setNullable(15, host?.netTxBps, Types.DOUBLE)
        statement.setNullable(16, host?.ioReadBps, Types.DOUBLE); statement.setNullable(17, host?.ioWriteBps, Types.DOUBLE)
        statement.setArray(18, statement.connection.createArrayOf("text", sample.missing.toTypedArray()))
    }

    override fun series(query: SeriesQuery): List<SeriesPoint> {
        ensureSchema()
        val from = query.from.toEpochMilli()
        val to = query.to.toEpochMilli()
        val step = query.step.toMillis()
        val rawSince = query.rawSince.toEpochMilli()
        // 5분 평균을 읽는 요청은 칸을 5분 경계에서 센다 — 요청 기간과 겹치는 구간은 통째로 한 점이고 점의 시각은 구간의 시작이다
        // (첫 점이 from 보다 최대 5분 앞설 수 있다 · §15.1). 간격([SeriesPlan])도 이 시작으로 골라져 온다
        val origin = if (from < rawSince) SeriesPlan.floorTo(query.from, BUCKET).toEpochMilli() else from
        val name = query.name ?: ""
        val cells = sortedMapOf<Long, MutableMap<String, MutableList<MetricAggregate>>>()
        fun collect(row: ResultSet) {
            cells.getOrPut(row.getLong("cell")) { linkedMapOf() }.getOrPut(row.getString("value_name")) { mutableListOf() } +=
                MetricAggregate(row.getDouble("average"), row.getDouble("maximum"), row.getLong("samples"))
        }
        // 7일보다 오래된 부분은 5분 평균에서 — 칸은 구간의 시작 시각으로 정한다. 큰 칸은 표본 수로 가중한 평균과 최댓값의 최댓값
        if (from < rawSince) this.query.query(
            """SELECT (bucket_ms - ?) / ? AS cell, value_name,
                      sum(average * samples) / sum(samples) AS average, max(maximum) AS maximum, sum(samples) AS samples
               FROM metric_rollup
               WHERE server = ? AND scope = ? AND name = ? AND bucket_ms > ? AND bucket_ms < ?
               GROUP BY 1, 2""",
            RowCallbackHandler { row -> collect(row) },
            origin, step, query.server, query.scope.wireName, name, from - BUCKET.toMillis(), minOf(to, rawSince),
        )
        // 나머지는 원본에서 — 컨테이너는 같은 항목 안의 두 자리를 먼저 더한 것이 표본 하나
        if (to > rawSince) this.query.query(
            """SELECT (measured_ms - ?) / ? AS cell, value_name, avg(value) AS average, max(value) AS maximum, count(value) AS samples
               FROM (${valueRows(query.scope)}) value_row
               WHERE server = ? AND name = ? AND measured_ms >= ? AND measured_ms < ?
               GROUP BY 1, 2 HAVING count(value) > 0""",
            RowCallbackHandler { row -> collect(row) },
            origin, step, query.server, name, maxOf(from, rawSince), to,
        )
        return cells.mapNotNull { (cell, values) ->
            val combined = values.mapNotNull { (valueName, parts) -> MetricAggregate.combine(parts)?.let { valueName to it } }.toMap()
            combined.takeIf { it.isNotEmpty() }?.let { SeriesPoint(Instant.ofEpochMilli(origin + cell * step), it) }
        }
    }

    override fun rollups(server: String, scope: MetricScope, name: String?, from: Instant, to: Instant): List<MetricRollup> {
        ensureSchema()
        val buckets = sortedMapOf<Long, MutableMap<String, MetricAggregate>>()
        query.query(
            """SELECT bucket_ms, value_name, average, maximum, samples FROM metric_rollup
               WHERE server = ? AND scope = ? AND name = ? AND bucket_ms >= ? AND bucket_ms < ?""",
            RowCallbackHandler { row ->
                buckets.getOrPut(row.getLong("bucket_ms")) { linkedMapOf() }[row.getString("value_name")] =
                    MetricAggregate(row.getDouble("average"), row.getDouble("maximum"), row.getLong("samples"))
            },
            server, scope.wireName, name ?: "", from.toEpochMilli(), to.toEpochMilli(),
        )
        return buckets.map { (bucket, values) -> MetricRollup(Instant.ofEpochMilli(bucket), values) }
    }

    override fun latest(receivedSince: Instant): List<ResourceSample> {
        ensureSchema()
        val heads = query.query(
            """SELECT DISTINCT ON (server) * FROM metric_sample WHERE received_ms >= ?
               ORDER BY server, measured_ms DESC, stream_ms DESC, stream_seq DESC""",
            { row, _ -> row.toSampleHead() },
            receivedSince.toEpochMilli(),
        )
        return heads.sortedBy { it.server }.map { head ->
            head.copy(
                containers = query.query(
                    "SELECT * FROM metric_container WHERE stream_ms = ? AND stream_seq = ? ORDER BY position",
                    { row, _ -> row.toContainer() },
                    head.id.millis, head.id.sequence,
                ),
                gpus = query.query(
                    "SELECT * FROM metric_gpu WHERE stream_ms = ? AND stream_seq = ? ORDER BY position",
                    { row, _ -> row.toGpu() },
                    head.id.millis, head.id.sequence,
                ),
            )
        }
    }

    /**
     * 임계값 판정의 입력 — 측정 시각이 `(from, to]` 인 항목을 컨테이너 · GPU 까지(서버 · 측정 시각순). 판정용 연결로 읽는다(§16).
     * 읽기만이라 5분 평균이 원본 표에 거는 SHARE 잠금에 막히지 않는다(읽기의 ACCESS SHARE 는 그것과 충돌하지 않는다).
     */
    override fun samplesMeasuredBetween(from: Instant, to: Instant): List<ResourceSample> {
        ensureSchema()
        val heads = judge.query(
            "SELECT * FROM metric_sample WHERE measured_ms > ? AND measured_ms <= ? ORDER BY server, measured_ms, stream_ms, stream_seq",
            { row, _ -> row.toSampleHead() }, from.toEpochMilli(), to.toEpochMilli(),
        )
        if (heads.isEmpty()) return heads
        fun <T> children(table: String, read: (ResultSet) -> T): Map<StreamId, List<T>> = judge.query(
            "SELECT * FROM $table WHERE measured_ms > ? AND measured_ms <= ? ORDER BY stream_ms, stream_seq, position",
            { row, _ -> StreamId(row.getLong("stream_ms"), row.getInt("stream_seq")) to read(row) }, from.toEpochMilli(), to.toEpochMilli(),
        ).groupBy({ it.first }, { it.second })
        val containers = children("metric_container") { it.toContainer() }
        val gpus = children("metric_gpu") { it.toGpu() }
        return heads.map { it.copy(containers = containers[it.id].orEmpty(), gpus = gpus[it.id].orEmpty()) }
    }

    /**
     * 서버별 마지막 수신 시각(판정의 입력 — 판정용 연결). 원본과 따로 둔 작은 표에서 읽는다 — 원본이 7일 뒤 지워져도 남는다(§15).
     * 새로 저장된 항목만 올린다(다시 전달된 항목은 치지 않는다 · §16).
     */
    override fun lastReceivedOfEveryServer(): Map<String, Instant> = receipts(judge)

    /** 서버별 마지막 수신 시각(`latest` 의 서버 목록 · `age_ms` — 조회 풀) */
    override fun serverReceipts(): Map<String, Instant> = receipts(query)

    private fun receipts(jdbc: JdbcTemplate): Map<String, Instant> {
        ensureSchema()
        return jdbc.query("SELECT server, last_received_ms FROM metric_server_receipt",
            { row, _ -> row.getString("server") to Instant.ofEpochMilli(row.getLong("last_received_ms")) }).toMap()
    }

    private fun ResultSet.toContainer() = ContainerValues(getString("name"), getString("container_id"), getString("project"), nullableDouble("cpu_pct"),
        nullableLong("mem_used"), nullableLong("restarts"), getString("state"))

    private fun ResultSet.toGpu() = GpuValues(getInt("gpu_index"), nullableDouble("util_pct"), nullableLong("mem_used"), nullableLong("mem_total"),
        nullableDouble("temp_c"))

    override fun containers(server: String, from: Instant, to: Instant): List<SeenContainer> {
        ensureSchema()
        return query.query(
            """SELECT series_name, max(measured_ms) AS last_seen_ms FROM metric_container
               WHERE server = ? AND measured_ms >= ? AND measured_ms <= ? GROUP BY series_name""",
            { row, _ -> SeenContainer(row.getString("series_name"), Instant.ofEpochMilli(row.getLong("last_seen_ms"))) },
            server, from.toEpochMilli(), to.toEpochMilli(),
        ).sortedBy { it.name }
    }

    /** 끝난 5분 구간을 모두 다시 계산할 만큼 돈다(개수 제한 없이) — 확정한 (서버, 구간) 수. 예약 실행은 [rollUpBuckets] 를 쓴다 */
    override fun rollUp(now: Instant): Int = rollUpBuckets(now, limit = null).computed

    /** §16 · §16.1: 구간마다 따로, 가장 오래 시도하지 않은 구간부터 [BUCKETS_PER_ROUND] 개 */
    override fun rollUpBuckets(now: Instant): RollupOutcome = rollUpBuckets(now, BUCKETS_PER_ROUND)

    /**
     * 다시 계산할 (서버, 5분 구간)을 고른다: 끝난 구간 중 평균이 없거나, 계산 시각이 그 구간 원본의 마지막 수신 시각보다 뒤가 아니거나,
     * 계산 뒤에 저장된 원본(저장 순번이 그때 본 것보다 큼)이 있는 구간. 차례는 마지막으로 시도한 시각(`metric_rollup_attempt` — "계산함" 표시와 따로,
     * 성패와 상관없이 남긴다)이 오래된 구간부터(시도한 적 없는 구간이 맨 앞, 그끼리는 서버 · 구간순) — 계속 실패하는 구간이 앞자리를 차지하지 않는다.
     * 구간마다 따로 한 트랜잭션([rollUpOne]) — 한 구간의 실패가 다른 구간을 되돌리지 않는다. 고르기가 실패하면(DB 에 닿지 못함 등) 예외.
     */
    private fun rollUpBuckets(now: Instant, limit: Int?): RollupOutcome {
        ensureSchema()
        val closedBefore = SeriesPlan.floorTo(now, BUCKET).toEpochMilli()
        val bucket = BUCKET.toMillis()
        val targets = ingest.query(
            """SELECT raw.server, raw.bucket_ms FROM (
                 SELECT server, measured_ms / ? * ? AS bucket_ms, max(received_ms) AS last_received, max(insert_order) AS last_order
                 FROM metric_sample WHERE measured_ms < ? GROUP BY 1, 2) raw
               LEFT JOIN metric_rollup_bucket done ON done.server = raw.server AND done.bucket_ms = raw.bucket_ms
               LEFT JOIN metric_rollup_attempt tried ON tried.server = raw.server AND tried.bucket_ms = raw.bucket_ms
               WHERE done.server IS NULL OR done.computed_ms <= raw.last_received OR done.insert_order_seen < raw.last_order
               ORDER BY tried.attempted_ms NULLS FIRST, raw.server, raw.bucket_ms
               LIMIT ?""",
            { row, _ -> row.getString("server") to row.getLong("bucket_ms") },
            bucket, bucket, closedBefore, limit ?: Int.MAX_VALUE,
        )
        var computed = 0
        val failures = mutableListOf<String>()
        var firstFailure: String? = null
        for ((server, bucketStart) in targets) {
            try {
                rollUpOne(server, bucketStart, now)
                computed++
            } catch (error: Exception) {
                failures += "$server ${Instant.ofEpochMilli(bucketStart)} ${error.javaClass.simpleName}"
                if (firstFailure == null) firstFailure = error.javaClass.simpleName
            }
            // 시도한 시각 — 성패와 상관없이(차례를 정한다). 이것이 실패해도 그 구간의 결과는 이미 정해졌다
            runCatching {
                ingest.update(
                    """INSERT INTO metric_rollup_attempt (server, bucket_ms, attempted_ms) VALUES (?,?,?)
                       ON CONFLICT (server, bucket_ms) DO UPDATE SET attempted_ms = EXCLUDED.attempted_ms""",
                    server, bucketStart, now.toEpochMilli(),
                )
            }
        }
        if (failures.isNotEmpty()) log.warn("observe:observe:metric rollup failed for {} buckets (first: {})", failures.size, failures.first())
        return RollupOutcome(computed, failures.size, firstFailure)
    }

    /**
     * (서버, 5분 구간) 하나를 한 트랜잭션에서 계산해 덮어쓰고 "계산함" 표시(계산 시각 · 그때 본 마지막 저장 순번)를 함께 확정한다.
     * 수신 시각은 수집기가 저장 **전에** 찍으므로 그것만으로는 "계산하는 동안 커밋된 원본"을 놓칠 수 있다 — 그래서 이 트랜잭션 동안
     * 원본 표에 SHARE 잠금을 걸어(읽기는 막지 않고 저장만 잠시 기다리게) 진행 중인 저장을 끝낸 뒤 보고, 그때의 마지막 저장 순번을 기록한다.
     * 잠금은 구간마다 잡고 풀므로 그 사이에 저장이 끼어들 수 있다 — 그렇게 들어온 원본은 기록된 순번보다 큰 순번을 받으므로 그 구간은
     * "다시 계산할 것"이 되고 원본 삭제의 조건(그때 본 순번 ≥ 그 구간의 마지막 순번)도 서지 않는다. 같은 구간을 두 자리가 함께 계산하면
     * 표시 줄을 먼저 쓴 쪽이 커밋할 때까지 다른 쪽이 그 줄에서 기다렸다가 이어서 덮어쓴다.
     */
    /**
     * 발번기의 마지막 값과 그 전제(§16.3) — 번호를 하나씩 내줘야(캐시 1) 마지막 값이 "지금까지 내준 번호"다. 캐시가 크면 한 연결이 미리 받아 둔
     * 번호를 나중에 쓰므로 그 값을 "이미 본 순번"으로 칠 수 없다. 캐시가 1 이 아니거나 값이 비어 오면(이름을 못 찾음 · 아직 내준 번호 없음) 예외 —
     * 그 구간은 실패로 남고(평균도 "계산함" 표시도 없다) 원본은 지워지지 않는다. 권한이 없으면 읽기 자체가 예외다.
     */
    private fun lastInsertOrder(): Long {
        val rows = ingest.queryForList(INSERT_ORDER_SEQUENCE)
        val row = rows.singleOrNull() ?: throw InsertOrderSequenceUnusable("metric_sample.insert_order 의 발번기를 찾지 못했다")
        val cache = (row["cache_size"] as Number).toLong()
        if (cache != 1L) throw InsertOrderSequenceUnusable("metric_sample.insert_order 의 발번기 캐시가 $cache 이다(1 이어야 한다)")
        return (row["last_value"] as Number?)?.toLong() ?: throw InsertOrderSequenceUnusable("metric_sample.insert_order 의 발번기 값이 비어 있다")
    }

    /** 발번기 읽기의 전제가 서지 않음 — 그 구간의 계산을 실패로(헬스의 실패 사유에 이 이름이 나온다) */
    private class InsertOrderSequenceUnusable(message: String) : IllegalStateException(message)

    private fun rollUpOne(server: String, bucketStart: Long, now: Instant) {
        val bucket = BUCKET.toMillis()
        transaction.executeWithoutResult {
            ingest.execute("LOCK TABLE metric_sample IN SHARE MODE")
            // 지금까지의 저장 순번 — 표를 훑지 않고 순번 발번기(identity 칸의 시퀀스)의 마지막 값을 읽는다(§16.2). 잠금을 쥔 동안에는
            // 진행 중인 저장이 없다(INSERT 는 표의 ROW EXCLUSIVE 잠금을 얻은 뒤에야 번호를 받는다) — 그래서 이 값은 저장된 모든 원본의 순번 이상이다.
            // 되돌려진 저장이 번호를 써 버려 더 커도, 뒤의 저장은 그보다 큰 번호를 받는다
            val seen = lastInsertOrder()
            ingest.update(
                """INSERT INTO metric_rollup_bucket (server, bucket_ms, computed_ms, insert_order_seen) VALUES (?,?,?,?)
                   ON CONFLICT (server, bucket_ms) DO UPDATE SET computed_ms = EXCLUDED.computed_ms, insert_order_seen = EXCLUDED.insert_order_seen""",
                server, bucketStart, now.toEpochMilli(), seen,
            )
            ingest.update("DELETE FROM metric_rollup WHERE server = ? AND bucket_ms = ?", server, bucketStart)
            for (scope in MetricScope.entries) {
                ingest.update(
                    """INSERT INTO metric_rollup (server, bucket_ms, scope, name, value_name, average, maximum, samples)
                       SELECT server, ?, ?, name, value_name, avg(value), max(value), count(value)
                       FROM (${valueRows(scope)}) value_row
                       WHERE server = ? AND measured_ms >= ? AND measured_ms < ?
                       GROUP BY server, name, value_name HAVING count(value) > 0""",
                    bucketStart, scope.wireName, server, bucketStart, bucketStart + bucket,
                )
            }
        }
    }

    /**
     * 원본: 구간 전체가 `now − 7일` 앞에 있고 그 구간의 평균이 원본을 모두 담은(계산 시각 > 마지막 수신, 그때 본 저장 순번 ≥ 마지막 저장 순번)
     * (서버, 5분 구간)의 원본만 지운다. 구간을 걸친 원본은 다음 날 구간째 지운다 — 일부만 지운 구간을 늦은 원본이 다시 계산하면
     * 지운 표본이 평균에서 빠지기 때문이다. 5분 평균: 구간 시작이 `now − 30일` 앞인 것. 원본을 먼저 지운다(평균이 있어야 지울 수 있으므로).
     */
    override fun deleteExpired(now: Instant): MetricDeletion {
        ensureSchema()
        val rawCutoff = now.minus(RAW_RETENTION)
        val wholeBucketsBefore = SeriesPlan.floorTo(rawCutoff, BUCKET).toEpochMilli()
        val bucket = BUCKET.toMillis()
        val rawDeleted = ingest.update(
            """DELETE FROM metric_sample sample
               USING (
                 SELECT done.server, done.bucket_ms FROM metric_rollup_bucket done
                 JOIN (SELECT server, measured_ms / ? * ? AS bucket_ms, max(received_ms) AS last_received, max(insert_order) AS last_order
                       FROM metric_sample WHERE measured_ms < ? GROUP BY 1, 2) raw
                   ON raw.server = done.server AND raw.bucket_ms = done.bucket_ms
                 WHERE done.computed_ms > raw.last_received AND done.insert_order_seen >= raw.last_order
               ) rolled
               WHERE sample.server = rolled.server
                 AND sample.measured_ms >= rolled.bucket_ms AND sample.measured_ms < rolled.bucket_ms + ?
                 AND sample.measured_ms < ?""",
            bucket, bucket, wholeBucketsBefore, bucket, rawCutoff.toEpochMilli(),
        )
        val rollupDeleted = ingest.update("DELETE FROM metric_rollup_bucket WHERE bucket_ms < ?", now.minus(ROLLUP_RETENTION).toEpochMilli())
        ingest.update("DELETE FROM metric_rollup_attempt WHERE bucket_ms < ?", now.minus(ROLLUP_RETENTION).toEpochMilli())   // 시도 기록도 같은 기한
        return MetricDeletion(rawDeleted, rollupDeleted)
    }

    /** 가장 최근에 저장된 항목의 수신 시각(헬스용) — 없으면 null, 표가 아직 없으면 SQLSTATE 42P01 예외, DB 에 닿지 못하면 예외 */
    fun latestStoredAt(): Instant? =
        health.queryForObject("SELECT max(received_ms) FROM metric_sample", Long::class.javaObjectType)?.let(Instant::ofEpochMilli)

    /** 서버별 마지막 수신 시각(헬스용 — §2.3) — 작은 표에서(헬스용 연결 · 표를 만들지 않는다) */
    fun lastReceivedByServer(): Map<String, Instant> =
        healthDetail.query("SELECT server, last_received_ms FROM metric_server_receipt ORDER BY server",
            { row, _ -> row.getString("server") to Instant.ofEpochMilli(row.getLong("last_received_ms")) }).toMap()

    /**
     * 서버마다 측정 시각이 가장 늦은 항목(컨테이너 · GPU 없이 — 헬스의 `clock_behind` 용, 헬스용 연결 · 표를 만들지 않는다).
     * 수신이 [receivedSince] 이후인 항목에서 고른다(`latest` 와 같은 범위).
     */
    fun latestHeads(receivedSince: Instant): List<ResourceSample> = healthDetail.query(
        """SELECT DISTINCT ON (server) * FROM metric_sample WHERE received_ms >= ?
           ORDER BY server, measured_ms DESC, stream_ms DESC, stream_seq DESC""",
        { row, _ -> row.toSampleHead() }, receivedSince.toEpochMilli(),
    )

    private fun ResultSet.toSampleHead(): ResourceSample = ResourceSample(
        id = StreamId(getLong("stream_ms"), getInt("stream_seq")),
        server = getString("server"),
        measuredAt = Instant.ofEpochMilli(getLong("measured_ms")),
        receivedAt = Instant.ofEpochMilli(getLong("received_ms")),
        intervalMs = nullableLong("interval_ms"),
        host = if (!getBoolean("host_present")) null else HostValues(
            nullableDouble("cpu_pct"), nullableDouble("load1"), nullableLong("mem_total"), nullableLong("mem_used"),
            nullableLong("disk_total"), nullableLong("disk_used"), nullableDouble("net_rx_bps"), nullableDouble("net_tx_bps"),
            nullableDouble("io_read_bps"), nullableDouble("io_write_bps"),
        ),
        containers = emptyList(),
        gpus = emptyList(),
        missing = (getArray("missing").array as Array<*>).map { it as String },
    )

    companion object {
        val RAW_RETENTION: Duration = Duration.ofDays(7)
        val ROLLUP_RETENTION: Duration = Duration.ofDays(30)
        val BUCKET: Duration = Duration.ofMinutes(5)
        /** 5분 평균 한 바퀴의 구간 수 상한(§16) — 밀린 뒤에도 저장이 오래 기다리지 않게 */
        const val BUCKETS_PER_ROUND = 200
        private val log = org.slf4j.LoggerFactory.getLogger("observe")

        /**
         * 순번 발번기의 캐시와 마지막 값 — 시퀀스는 표 이름으로 찾는다(검색 경로를 따라 풀린다). 못 찾으면 줄이 없고, 아직 내준 번호가 없으면 값이 null.
         * `pg_sequence` 는 누구나 읽는 목록이고, 값 읽기(`pg_sequence_last_value`)는 그 시퀀스의 권한이 있어야 한다(없으면 예외)
         */
        private const val INSERT_ORDER_SEQUENCE =
            """SELECT sequence.seqcache AS cache_size, pg_sequence_last_value(sequence.seqrelid) AS last_value
               FROM pg_sequence sequence
               WHERE sequence.seqrelid = pg_get_serial_sequence('metric_sample', 'insert_order')::regclass"""
        private const val UPSERT_RECEIPT =
            """INSERT INTO metric_server_receipt (server, last_received_ms) VALUES (?, ?)
               ON CONFLICT (server) DO UPDATE SET last_received_ms = GREATEST(metric_server_receipt.last_received_ms, EXCLUDED.last_received_ms)"""
        private const val INSERT_SAMPLE =
            """INSERT INTO metric_sample (stream_ms, stream_seq, server, measured_ms, received_ms, interval_ms, host_present,
                 cpu_pct, load1, mem_total, mem_used, disk_total, disk_used, net_rx_bps, net_tx_bps, io_read_bps, io_write_bps, missing)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (stream_ms, stream_seq) DO NOTHING"""
        private const val INSERT_CONTAINER =
            """INSERT INTO metric_container (stream_ms, stream_seq, position, server, measured_ms, name, series_name, container_id, project,
                 cpu_pct, mem_used, restarts, state)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (stream_ms, stream_seq, position) DO NOTHING"""
        private const val INSERT_GPU =
            """INSERT INTO metric_gpu (stream_ms, stream_seq, position, server, measured_ms, gpu_index, util_pct, mem_used, mem_total, temp_c)
               VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (stream_ms, stream_seq, position) DO NOTHING"""

        /** 갈래마다 표본 하나 = 한 행으로 맞춘 원본(칸: server · measured_ms · name · 값 이름마다 한 칸) — 컨테이너는 같은 항목의 두 자리를 더한다 */
        private fun samplesOf(scope: MetricScope): String = when (scope) {
            MetricScope.HOST -> "SELECT server, measured_ms, '' AS name, ${scope.values.joinToString { "$it::float8 AS $it" }} FROM metric_sample"
            MetricScope.CONTAINER ->
                """SELECT server, measured_ms, series_name AS name, ${scope.values.joinToString { "sum($it)::float8 AS $it" }}
                   FROM metric_container GROUP BY server, stream_ms, stream_seq, measured_ms, series_name"""
            MetricScope.GPU -> "SELECT server, measured_ms, gpu_index::text AS name, ${scope.values.joinToString { "$it::float8 AS $it" }} FROM metric_gpu"
        }

        /** 값 이름마다 한 행으로 편 원본(칸: server · measured_ms · name · value_name · value). 값 이름은 [MetricScope.values] 의 고정 글자다(입력이 아니다) */
        private fun valueRows(scope: MetricScope): String =
            """SELECT sample.server, sample.measured_ms, sample.name, value_column.value_name, value_column.value
               FROM (${samplesOf(scope)}) sample
               CROSS JOIN LATERAL (VALUES ${scope.values.joinToString { "('$it', sample.$it)" }}) value_column (value_name, value)"""

        private fun PreparedStatement.setNullable(index: Int, value: Any?, sqlType: Int) =
            if (value == null) setNull(index, sqlType) else setObject(index, value, sqlType)

        private fun ResultSet.nullableDouble(column: String): Double? = getDouble(column).takeUnless { wasNull() }
        private fun ResultSet.nullableLong(column: String): Long? = getLong(column).takeUnless { wasNull() }
    }
}
