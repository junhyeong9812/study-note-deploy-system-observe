package xyz.junproject.observe.visit

import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.sql.Types

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
    /** [cutoff] 보다 오래된 줄을 한 문장으로 많아야 [limit] 줄 지운다. 지운 줄 수. 실패는 예외 — 나눠 지우기는 부르는 쪽([VisitSchedule])이 잇는다 */
    fun deleteOlderThan(cutoff: Instant, limit: Int): Int
}

/**
 * PostgreSQL 저장소 — 로그와 같은 DB, 표는 추가만. 수집(+ 하루 한 번의 삭제) · 조회 · 헬스의 연결 풀을 나눈다.
 *
 * 표 `visit_event`: 스트림 항목 하나 = 한 줄(기본 키 = 스트림 ID — 같은 항목을 두 번 받아도 한 줄). 방문 시각은 `stream_ms`(Redis 가 받은 시각).
 * `referrer_host` 가 null 이면 직접. **`ip` 는 주소 원문이고 `visitor` 는 그날의 구분값이다 — 이 표에만 있다**: 요약은 이 둘을 세기만 하고
 * 고르지 않으며(응답에 싣지 않는다), 저장 실패의 예외 글(넣으려던 값이 실린다)은 부르는 쪽이 종류 이름만 남긴다.
 */
class VisitStore(private val ingest: JdbcTemplate, private val query: JdbcTemplate, private val health: JdbcTemplate) : VisitSink, VisitReader, VisitRetention {

    /** 스키마 — 있으면 그대로(`IF NOT EXISTS`). 두 자리가 동시에 처음 만들면 한쪽이 실패할 수 있다 — 그 바퀴가 실패로 남고 다음 바퀴가 다시 만든다 */
    override fun createSchema() {
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS visit_event (
                 stream_ms     bigint  NOT NULL,
                 stream_seq    integer NOT NULL,
                 path          text    NOT NULL,
                 screen        text    NOT NULL,
                 referrer_host text,
                 device        text    NOT NULL,
                 visitor       text    NOT NULL,
                 ip            text    NOT NULL,
                 PRIMARY KEY (stream_ms, stream_seq))"""
        )
    }

    override fun insertAll(visits: List<Visit>): Set<StreamId> {
        if (visits.isEmpty()) return emptySet()
        val results = ingest.batchUpdate(
            """INSERT INTO visit_event (stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip)
               VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (stream_ms, stream_seq) DO NOTHING""",
            visits.map { arrayOf<Any?>(it.id.millis, it.id.sequence, it.path, it.screen, it.referrerHost, it.device.wireName, it.visitor, it.ip) },
            intArrayOf(Types.BIGINT, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
        )
        return visits.filterIndexed { index, _ -> results[index] > 0 || results[index] == java.sql.Statement.SUCCESS_NO_INFO }.map { it.id }.toSet()
    }

    /**
     * 서울 시각으로 오늘을 포함한 [days] 날 — 첫날 0시부터 [now] 까지(양 끝 포함). 날짜별 줄은 방문이 없는 날도 0 으로 있다.
     * 많이 본 경로 · 들어온 곳은 봇을 빼고 상위 [TOP_LIMIT] 개(같은 수는 글자의 바이트 순 — `COLLATE "C"`, 직접(null)은 맨 앞), 하나 더 읽어 넘치는지 본다.
     */
    override fun summary(days: Int, now: Instant): VisitSummary {
        val firstDay = LocalDate.ofInstant(now, ZONE).minusDays(days - 1L)
        val from = firstDay.atStartOfDay(ZONE).toInstant()
        val range = arrayOf<Any>(from.toEpochMilli(), now.toEpochMilli())
        val counted = query.query(
            """SELECT ((to_timestamp(0) + stream_ms * interval '1 millisecond') AT TIME ZONE ?)::date AS day,
                      count(*) FILTER (WHERE device <> 'bot') AS views,
                      count(*) AS views_with_bots,
                      count(DISTINCT visitor) FILTER (WHERE device <> 'bot') AS visitors,
                      count(DISTINCT visitor) AS visitors_with_bots
               FROM visit_event WHERE stream_ms >= ? AND stream_ms <= ? GROUP BY 1""",
            { row, _ ->
                VisitDay(row.getObject("day", LocalDate::class.java), row.getLong("views"), row.getLong("views_with_bots"),
                    row.getLong("visitors"), row.getLong("visitors_with_bots"))
            },
            ZONE.id, *range,
        ).associateBy { it.date }
        val daily = (0 until days).map { offset -> firstDay.plusDays(offset.toLong()).let { date -> counted[date] ?: VisitDay(date, 0, 0, 0, 0) } }
        val paths = query.query(
            """SELECT path, count(*) AS views FROM visit_event WHERE stream_ms >= ? AND stream_ms <= ? AND device <> 'bot'
               GROUP BY path ORDER BY views DESC, path COLLATE "C" LIMIT ?""",
            { row, _ -> PathViews(row.getString("path"), row.getLong("views")) }, *range, TOP_LIMIT + 1,
        )
        val referrers = query.query(
            """SELECT referrer_host, count(*) AS views FROM visit_event WHERE stream_ms >= ? AND stream_ms <= ? AND device <> 'bot'
               GROUP BY referrer_host ORDER BY views DESC, referrer_host COLLATE "C" NULLS FIRST LIMIT ?""",
            { row, _ -> ReferrerViews(row.getString("referrer_host"), row.getLong("views")) }, *range, TOP_LIMIT + 1,
        )
        val devices = query.query(
            "SELECT device, count(*) AS views FROM visit_event WHERE stream_ms >= ? AND stream_ms <= ? GROUP BY device",
            { row, _ -> row.getString("device") to row.getLong("views") }, *range,
        ).toMap()
        return VisitSummary(
            days = days, from = from, to = now, daily = daily,
            topPaths = paths.take(TOP_LIMIT), topPathsTruncated = paths.size > TOP_LIMIT,
            topReferrers = referrers.take(TOP_LIMIT), topReferrersTruncated = referrers.size > TOP_LIMIT,
            devices = VisitDevice.entries.associateWith { devices[it.wireName] ?: 0L },
        )
    }

    /**
     * PostgreSQL 에는 `DELETE … LIMIT` 이 없다 — 기본 키(`stream_ms` 가 앞 칸)로 고른 [limit] 줄의 줄 위치(ctid)로 지운다.
     * `ORDER BY stream_ms` 는 오래된 것부터 지우려는 것이자, 같은 연결에서 문장이 되풀이되어 값과 무관한 일반 계획으로 바뀌어도
     * 기본 키 색인을 타게 하려는 것이다(없으면 일반 계획이 표 전체 훑기로 바뀐다 — EXPLAIN 으로 확인).
     */
    override fun deleteOlderThan(cutoff: Instant, limit: Int): Int = ingest.update(
        "DELETE FROM visit_event WHERE ctid IN (SELECT ctid FROM visit_event WHERE stream_ms < ? ORDER BY stream_ms LIMIT ?)", cutoff.toEpochMilli(), limit,
    )

    /** 가장 오래된 방문의 시각(헬스용) — 비었으면 null, 표가 없으면 SQLSTATE 42P01 예외, DB 에 닿지 못하면 예외 */
    fun oldestStoredAt(): Instant? = health.queryForObject("SELECT min(stream_ms) FROM visit_event", Long::class.javaObjectType)?.let(Instant::ofEpochMilli)

    /** 가장 최근에 저장된 방문의 시각(헬스용) — 비었으면 null, 표가 없으면 SQLSTATE 42P01 예외, DB 에 닿지 못하면 예외 */
    fun latestStoredAt(): Instant? = health.query(
        "SELECT stream_ms FROM visit_event ORDER BY stream_ms DESC, stream_seq DESC LIMIT 1",
        { row, _ -> Instant.ofEpochMilli(row.getLong("stream_ms")) },
    ).firstOrNull()

    companion object {
        val RETENTION: Duration = Duration.ofDays(90)
        val ZONE: ZoneId = ZoneId.of("Asia/Seoul")
        const val TOP_LIMIT = 10
        val SUMMARY_DAYS = setOf(7, 30, 90)
    }
}
