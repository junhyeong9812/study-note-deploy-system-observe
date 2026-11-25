package xyz.junproject.observe.visit

import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

/** 하루(서울 시각의 날) — 봇 제외 / 봇 포함 */
data class VisitDay(val date: LocalDate, val views: Long, val viewsWithBots: Long, val visitors: Long, val visitorsWithBots: Long)

data class PathViews(val path: String, val views: Long)

/** [host] 가 null 이면 직접(들어온 곳 없음) */
data class ReferrerViews(val host: String?, val views: Long)

data class VisitSummary(
    val days: Int,
    val from: Instant,
    val to: Instant,
    val daily: List<VisitDay>,
    val topPaths: List<PathViews>,
    val topPathsTruncated: Boolean,
    val topReferrers: List<ReferrerViews>,
    val topReferrersTruncated: Boolean,
    val devices: Map<VisitDevice, Long>,
)

/** 조회 쪽이 쓰는 면 */
interface VisitReader {
    /** 서울 시각으로 오늘을 포함한 [days] 날의 요약 — [now] 까지 */
    fun summary(days: Int, now: Instant): VisitSummary
}

/** 보관 삭제의 면 */
interface VisitRetention {
    /** [cutoff] 보다 오래된 줄을 지운다. 지운 줄 수. 실패는 예외 */
    fun deleteOlderThan(cutoff: Instant): Int
}

class VisitStore(private val ingest: JdbcTemplate, private val query: JdbcTemplate, private val health: JdbcTemplate) : VisitSink, VisitReader, VisitRetention {
    override fun createSchema(): Unit = TODO("visit-tracking V2 구현")
    override fun insertAll(visits: List<Visit>): Int = TODO("visit-tracking V2 구현")
    override fun summary(days: Int, now: Instant): VisitSummary = TODO("visit-tracking V2 구현")
    override fun deleteOlderThan(cutoff: Instant): Int = TODO("visit-tracking V2 구현")
    /** 가장 최근에 저장된 방문의 시각(헬스용) — 비었으면 null, 표가 없으면 SQLSTATE 42P01 예외, DB 에 닿지 못하면 예외 */
    fun latestStoredAt(): Instant? = TODO("visit-tracking V2 구현")

    companion object {
        val RETENTION: Duration = Duration.ofDays(90)
        val ZONE: ZoneId = ZoneId.of("Asia/Seoul")
        const val TOP_LIMIT = 10
        val SUMMARY_DAYS = setOf(7, 30, 90)
    }
}
