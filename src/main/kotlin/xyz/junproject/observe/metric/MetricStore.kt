package xyz.junproject.observe.metric

import org.springframework.jdbc.core.JdbcTemplate
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
}

/** 원본 7일 · 5분 평균 30일 삭제 한 번의 결과 — [rawDeleted] 는 지운 항목(스트림 항목) 수, [rollupDeleted] 는 지운 (서버, 5분 구간) 수 */
data class MetricDeletion(val rawDeleted: Int, val rollupDeleted: Int)

/** 지표 삭제 한 번의 결과(헬스용) — 실패면 [deletion] 은 null 이고 [failure] 에 예외 종류 이름 */
data class MetricRetentionResult(val ranAt: Instant, val deletion: MetricDeletion?, val failure: String?)

/** 5분 평균 한 바퀴의 결과(헬스용) — 실패면 [buckets] 는 null 이고 [failure] 에 예외 종류 이름 */
data class MetricRollupResult(val ranAt: Instant, val buckets: Int?, val failure: String?)

/** 예약 실행이 쓰는 면 — 5분 평균과 보관 삭제. 시각은 늘 인자로 받는다(테스트가 시계를 정한다) */
interface MetricMaintenance {
    /**
     * 끝난 5분 구간(`bucketStart + 5분 ≤ now`) 중 **평균이 없거나, 평균의 계산 시각이 그 구간 원본의 마지막 수신 시각보다 뒤가 아닌**
     * (서버, 구간)을 원본에서 다시 계산해 덮어쓴다. 계산 시각은 [now]. 여러 번 돌려도 결과가 같다. 계산한 (서버, 구간) 수를 돌려준다.
     * 원본 값이 모두 null 인 구간도 "계산함"으로 남긴다(그래야 그 원본을 지울 수 있다).
     */
    fun rollUp(now: Instant): Int

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
 */
class MetricStore(
    private val ingest: JdbcTemplate,
    private val query: JdbcTemplate,
    private val health: JdbcTemplate,
) : MetricSink, MetricReader, MetricMaintenance {

    override fun createSchema(): Unit = TODO("not implemented")
    override fun insertAll(samples: List<ResourceSample>): Int = TODO("not implemented")
    override fun series(query: SeriesQuery): List<SeriesPoint> = TODO("not implemented")
    override fun rollups(server: String, scope: MetricScope, name: String?, from: Instant, to: Instant): List<MetricRollup> = TODO("not implemented")
    override fun latest(receivedSince: Instant): List<ResourceSample> = TODO("not implemented")
    override fun containers(server: String, from: Instant, to: Instant): List<SeenContainer> = TODO("not implemented")
    override fun rollUp(now: Instant): Int = TODO("not implemented")
    override fun deleteExpired(now: Instant): MetricDeletion = TODO("not implemented")

    /** 가장 최근에 저장된 항목의 수신 시각(헬스용) — 없으면 null, 표가 아직 없으면 SQLSTATE 42P01 예외, DB 에 닿지 못하면 예외 */
    fun latestStoredAt(): Instant? = TODO("not implemented")

    /** 서버별 마지막 수신 시각(헬스용 — §2.3) */
    fun lastReceivedByServer(): Map<String, Instant> = TODO("not implemented")

    companion object {
        val RAW_RETENTION: Duration = Duration.ofDays(7)
        val ROLLUP_RETENTION: Duration = Duration.ofDays(30)
        val BUCKET: Duration = Duration.ofMinutes(5)
    }
}
