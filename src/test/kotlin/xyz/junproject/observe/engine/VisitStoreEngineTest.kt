package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.visit.PathViews
import xyz.junproject.observe.visit.ReferrerViews
import xyz.junproject.observe.visit.Visit
import xyz.junproject.observe.visit.VisitDay
import xyz.junproject.observe.visit.VisitDevice
import xyz.junproject.observe.visit.VisitInsertion
import xyz.junproject.observe.visit.VisitRecords
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V6
import xyz.junproject.observe.visit.VisitRecords.VISITOR_ONE
import xyz.junproject.observe.visit.VisitRecords.VISITOR_TWO
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.metric.FakeMetricStream
import xyz.junproject.observe.visit.FakeVisitSink
import xyz.junproject.observe.visit.VisitCollector
import xyz.junproject.observe.visit.VisitRetention
import xyz.junproject.observe.visit.VisitSchedule
import xyz.junproject.observe.visit.VisitStore
import java.time.Duration
import java.time.Instant
import java.time.LocalDate

/**
 * 실제 PostgreSQL — 방문 표 `visit_event`(한 항목 = 한 줄, 기본 키 = 스트림 ID) · 90일 보관의 경계 · 요약(서울 시각의 날).
 * 표의 칸(계약): `stream_ms` bigint · `stream_seq` integer · `path` · `screen` · `referrer_host`(null = 직접) · `device` · `visitor` · `ip` — 글자 칸은 text.
 * 표는 전용 스키마(`visit_store`)에 만든다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VisitStoreEngineTest {
    private val schema = "visit_store"
    private val administration = JdbcTemplate(EngineSupport.dataSource(size = 1))
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@VisitStoreEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 3
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = VisitStore(jdbc, jdbc, jdbc)

    /** 2026-11-25 12:00 서울 */
    private val now = Instant.parse("2026-11-25T03:00:00Z")

    @BeforeEach
    fun reset() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administration.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administration.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        (administration.dataSource as HikariDataSource).close()
    }

    private fun rows(): List<Map<String, Any?>> =
        jdbc.queryForList("SELECT stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip FROM visit_event ORDER BY stream_ms, stream_seq")

    private fun at(text: String) = Instant.parse(text).toEpochMilli()

    // ── 저장 ──────────────────────────────────────────────

    @Test
    fun `한 항목은 한 줄이고 칸 값이 넣은 그대로다`() {
        val visits = listOf(
            VisitRecords.visit(1_764_000_000_000, 0),
            VisitRecords.visit(1_764_000_000_000, 1, path = "/wiki/쿠버네티스", screen = "search", referrerHost = null, device = VisitDevice.MOBILE,
                visitor = VISITOR_TWO, ip = ADDRESS_V6),
        )
        assertEquals(VisitInsertion(visits.map { it.id }.toSet(), emptySet()), store.insertAll(visits))
        assertEquals(listOf(
            mapOf("stream_ms" to 1_764_000_000_000L, "stream_seq" to 0, "path" to "/wiki/kubernetes/pod", "screen" to "topic", "referrer_host" to "www.google.com",
                "device" to "desktop", "visitor" to VISITOR_ONE, "ip" to ADDRESS_V4),
            mapOf("stream_ms" to 1_764_000_000_000L, "stream_seq" to 1, "path" to "/wiki/쿠버네티스", "screen" to "search", "referrer_host" to null,
                "device" to "mobile", "visitor" to VISITOR_TWO, "ip" to ADDRESS_V6),
        ), rows())
    }

    @Test
    fun `같은 스트림 ID 를 두 번 저장해도 한 줄이고 처음 것이 남는다`() {
        assertEquals(setOf(StreamId(1_000, 0)), store.insertAll(listOf(VisitRecords.visit(1_000))).inserted)
        assertEquals(VisitInsertion(emptySet(), emptySet()), store.insertAll(listOf(VisitRecords.visit(1_000, path = "/other"))))
        assertEquals(VisitInsertion(setOf(StreamId(2_000, 0)), emptySet()), store.insertAll(listOf(VisitRecords.visit(1_000), VisitRecords.visit(2_000))), "이미 있던 것까지 새로 넣었다고 했다")
        assertEquals(listOf(1_000L, 2_000L), rows().map { it["stream_ms"] })
        assertEquals("/wiki/kubernetes/pod", rows().first()["path"])
    }

    @Test
    fun `스키마는 몇 번을 만들어도 그대로다`() {
        store.insertAll(listOf(VisitRecords.visit(1_000)))
        store.createSchema()
        assertEquals(1, rows().size)
    }

    @Test
    fun `가장 최근 저장 시각은 스트림 ID 의 밀리초이고 비었으면 null`() {
        assertNull(store.latestStoredAt())
        store.insertAll(listOf(VisitRecords.visit(at("2026-11-25T02:00:00Z")), VisitRecords.visit(at("2026-11-25T02:59:59.123Z"))))
        assertEquals(Instant.parse("2026-11-25T02:59:59.123Z"), store.latestStoredAt())
    }

    // ── 90일 보관 ────────────────────────────────────────

    @Test
    fun `90일이 지난 줄은 지우고 89일 23시간 59분 된 줄과 정확히 90일 된 줄은 남긴다`() {
        val cutoff = now.minus(VisitStore.RETENTION)
        assertEquals(Duration.ofDays(90), VisitStore.RETENTION)
        store.insertAll(listOf(
            VisitRecords.visit(now.minus(Duration.ofDays(90)).minus(Duration.ofMinutes(1)).toEpochMilli()),
            VisitRecords.visit(cutoff.toEpochMilli() - 1),
            VisitRecords.visit(cutoff.toEpochMilli()),
            VisitRecords.visit(now.minus(Duration.ofDays(89)).minus(Duration.ofHours(23)).minus(Duration.ofMinutes(59)).toEpochMilli()),
            VisitRecords.visit(now.toEpochMilli()),
        ))
        assertEquals(2, store.deleteOlderThan(cutoff, 5_000))
        assertEquals(listOf(cutoff.toEpochMilli(), now.minus(Duration.ofMinutes(89L * 24 * 60 + 23 * 60 + 59)).toEpochMilli(), now.toEpochMilli()),
            rows().map { it["stream_ms"] })
        assertEquals(0, store.deleteOlderThan(cutoff, 5_000), "두 번째 삭제가 또 지웠다")
    }

    /** 기한(`cutoff`)보다 1ms 부터 [count] ms 까지 오래된 줄을 한 문장으로 넣는다 — 모두 기한을 넘겼다 */
    private fun insertOverdue(cutoff: Instant, count: Int) {
        jdbc.update(
            """INSERT INTO visit_event (stream_ms, stream_seq, path, screen, referrer_host, device, visitor, ip)
               SELECT ? - n, 0, '/old', 'topic', NULL, 'desktop', ?, ? FROM generate_series(1, ?) AS n""",
            cutoff.toEpochMilli(), VISITOR_ONE, ADDRESS_V4, count,
        )
    }

    private fun overdueCount(cutoff: Instant) = jdbc.queryForObject("SELECT count(*) FROM visit_event WHERE stream_ms < ?", Long::class.java, cutoff.toEpochMilli())!!

    @Test
    fun `한 문장은 묶음 크기까지만 지우고 기한 안의 줄은 건드리지 않는다`() {
        val cutoff = now.minus(VisitStore.RETENTION)
        insertOverdue(cutoff, 12_000)
        store.insertAll(listOf(VisitRecords.visit(cutoff.toEpochMilli()), VisitRecords.visit(cutoff.toEpochMilli() + 1), VisitRecords.visit(now.toEpochMilli())))
        assertEquals(5_000, store.deleteOlderThan(cutoff, 5_000))
        assertEquals(7_000L, overdueCount(cutoff), "한 문장이 묶음 크기보다 많이(또는 적게) 지웠다")
        assertEquals(3L, jdbc.queryForObject("SELECT count(*) FROM visit_event WHERE stream_ms >= ?", Long::class.java, cutoff.toEpochMilli()), "기한 안의 줄을 지웠다")
        assertEquals(5_000, store.deleteOlderThan(cutoff, 5_000))
        assertEquals(2_000, store.deleteOlderThan(cutoff, 5_000), "남은 것보다 많이 지웠다고 했다")
        assertEquals(0, store.deleteOlderThan(cutoff, 5_000))
        assertEquals(3L, jdbc.queryForObject("SELECT count(*) FROM visit_event", Long::class.java), "기한 안의 줄을 지웠다")
    }

    @Test
    fun `밀린 줄이 묶음 크기보다 많아도 한 번의 실행이 나눠 모두 지우고 기한 안의 줄은 경계의 줄까지 남긴다`() {
        val cutoff = now.minus(Duration.ofDays(90))
        insertOverdue(cutoff, 12_000)
        val kept = listOf(cutoff.toEpochMilli(), cutoff.toEpochMilli() + 1, now.toEpochMilli())   // 정확히 90일 된 줄 · 그보다 1ms 새 줄 · 지금
        store.insertAll(kept.map { VisitRecords.visit(it) })
        val deletedPerStatement = mutableListOf<Int>()
        val recording = object : VisitRetention {
            override fun deleteOlderThan(cutoff: Instant, limit: Int) = store.deleteOlderThan(cutoff, limit).also { deletedPerStatement += it }
        }
        val schedule = VisitSchedule(VisitCollector(FakeMetricStream(), FakeVisitSink(), block = Duration.ZERO), recording,
            retentionBatchSize = 5_000, retentionTimeBudget = Duration.ofMinutes(5))
        assertEquals(12_000, schedule.deleteOldVisits(now))
        assertEquals(listOf(5_000, 5_000, 2_000), deletedPerStatement, "문장마다 지운 수")
        assertEquals(0L, overdueCount(cutoff), "기한을 넘긴 줄이 남았다")
        assertEquals(kept, rows().map { it["stream_ms"] }, "기한 안의 줄이 지워졌다")
        assertEquals(RetentionResult(now, 12_000, null), schedule.lastRetention)
    }

    @Test
    fun `가장 오래된 줄의 시각은 스트림 ID 의 밀리초이고 비었으면 null`() {
        assertNull(store.oldestStoredAt())
        store.insertAll(listOf(VisitRecords.visit(at("2026-11-25T02:00:00Z"), 1), VisitRecords.visit(at("2026-08-01T00:00:00.123Z")), VisitRecords.visit(at("2026-11-25T02:00:00Z"))))
        assertEquals(Instant.parse("2026-08-01T00:00:00.123Z"), store.oldestStoredAt())
    }

    // ── 요약 ──────────────────────────────────────────────

    @Test
    fun `날짜별 줄은 서울 시각의 날로 나누고 방문이 없는 날도 0 으로 있다`() {
        store.insertAll(listOf(
            VisitRecords.visit(at("2026-11-24T14:59:59.999Z")),                       // 11-24 23:59:59.999 서울
            VisitRecords.visit(at("2026-11-24T15:00:00Z")),                           // 11-25 00:00 서울
            VisitRecords.visit(at("2026-11-25T02:00:00Z"), visitor = VISITOR_TWO),
            VisitRecords.visit(at("2026-11-18T15:00:00Z")),                           // 7일 창의 첫날(11-19) 0시
            VisitRecords.visit(at("2026-11-18T14:59:59.999Z")),                       // 그 앞 — 창 밖
        ))
        val summary = store.summary(7, now)
        assertEquals(7, summary.days)
        assertEquals(Instant.parse("2026-11-18T15:00:00Z"), summary.from)
        assertEquals(now, summary.to)
        assertEquals((19..25).map { LocalDate.parse("2026-11-$it") }, summary.daily.map { it.date }, "빈 날을 건너뛰었거나 날이 틀렸다")
        assertEquals(listOf(1L, 0L, 0L, 0L, 0L, 1L, 2L), summary.daily.map { it.views })
        assertEquals(VisitDay(LocalDate.parse("2026-11-25"), 2, 2, 2, 2), summary.daily.last())
        assertEquals(VisitDay(LocalDate.parse("2026-11-20"), 0, 0, 0, 0), summary.daily[1])
    }

    @Test
    fun `30일과 90일 창은 그만큼의 날 줄을 낸다`() {
        assertEquals(30, store.summary(30, now).daily.size)
        val ninety = store.summary(90, now)
        assertEquals(90, ninety.daily.size)
        assertEquals(LocalDate.parse("2026-08-28"), ninety.daily.first().date)
        assertEquals(LocalDate.parse("2026-11-25"), ninety.daily.last().date)
    }

    @Test
    fun `순방문자는 그날의 구분값의 서로 다른 수이고 봇 제외와 봇 포함을 가른다`() {
        val bot = "b0b0b0b0" + "0".repeat(56)
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll(listOf(
            VisitRecords.visit(day, 0, visitor = VISITOR_ONE),
            VisitRecords.visit(day, 1, visitor = VISITOR_ONE, path = "/wiki/b"),
            VisitRecords.visit(day, 2, visitor = VISITOR_TWO, device = VisitDevice.MOBILE),
            VisitRecords.visit(day, 3, visitor = bot, device = VisitDevice.BOT),
            VisitRecords.visit(day, 4, visitor = bot, device = VisitDevice.BOT),
            VisitRecords.visit(at("2026-11-24T01:00:00Z"), visitor = VISITOR_ONE),   // 전날의 같은 구분값은 전날에만
        ))
        val daily = store.summary(7, now).daily
        assertEquals(VisitDay(LocalDate.parse("2026-11-25"), views = 3, viewsWithBots = 5, visitors = 2, visitorsWithBots = 3), daily.last())
        assertEquals(VisitDay(LocalDate.parse("2026-11-24"), views = 1, viewsWithBots = 1, visitors = 1, visitorsWithBots = 1), daily[daily.size - 2])
    }

    @Test
    fun `많이 본 경로는 봇을 빼고 상위 10 개이며 넘치면 truncated 이다`() {
        val day = at("2026-11-25T01:00:00Z")
        var sequence = 0
        val visits = mutableListOf<Visit>()
        // /p00 은 12번, /p01 은 11번 … /p10 은 1번 — 서로 다른 경로 11개
        for (index in 0..10) repeat(12 - index) { visits += VisitRecords.visit(day, sequence++, path = "/p%02d".format(index)) }
        repeat(50) { visits += VisitRecords.visit(day, sequence++, path = "/bot-only", device = VisitDevice.BOT) }
        store.insertAll(visits)
        val summary = store.summary(7, now)
        assertEquals((0..9).map { PathViews("/p%02d".format(it), 12L - it) }, summary.topPaths)
        assertTrue(summary.topPathsTruncated)
        assertFalse(summary.topPaths.any { it.path == "/bot-only" }, "봇의 경로가 섞였다")
    }

    @Test
    fun `상위 경로가 10 개 이하면 truncated 가 아니고 같은 수는 경로 순이다`() {
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll(listOf(
            VisitRecords.visit(day, 0, path = "/b"), VisitRecords.visit(day, 1, path = "/a"), VisitRecords.visit(day, 2, path = "/c"), VisitRecords.visit(day, 3, path = "/c"),
        ))
        val summary = store.summary(7, now)
        assertEquals(listOf(PathViews("/c", 2), PathViews("/a", 1), PathViews("/b", 1)), summary.topPaths)
        assertFalse(summary.topPathsTruncated)
    }

    @Test
    fun `들어온 곳은 봇을 빼고 호스트별로 세며 없음은 직접(null)으로 묶는다`() {
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll(listOf(
            VisitRecords.visit(day, 0, referrerHost = null), VisitRecords.visit(day, 1, referrerHost = null), VisitRecords.visit(day, 2, referrerHost = null),
            VisitRecords.visit(day, 3, referrerHost = "www.google.com"), VisitRecords.visit(day, 4, referrerHost = "www.google.com"),
            VisitRecords.visit(day, 5, referrerHost = "github.com"),
            VisitRecords.visit(day, 6, referrerHost = "spam.example", device = VisitDevice.BOT),
        ))
        val summary = store.summary(7, now)
        assertEquals(listOf(ReferrerViews(null, 3), ReferrerViews("www.google.com", 2), ReferrerViews("github.com", 1)), summary.topReferrers)
        assertFalse(summary.topReferrersTruncated)
    }

    @Test
    fun `들어온 곳도 상위 10 개에서 자르고 truncated 를 낸다`() {
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll((0..10).map { VisitRecords.visit(day, it, referrerHost = "host$it.example") })
        val summary = store.summary(7, now)
        assertEquals(10, summary.topReferrers.size)
        assertTrue(summary.topReferrersTruncated)
    }

    @Test
    fun `기기 갈래별 수는 창 안의 전부이고 세 갈래가 늘 있다`() {
        assertEquals(mapOf(VisitDevice.DESKTOP to 0L, VisitDevice.MOBILE to 0L, VisitDevice.BOT to 0L), store.summary(7, now).devices)
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll(listOf(
            VisitRecords.visit(day, 0), VisitRecords.visit(day, 1), VisitRecords.visit(day, 2, device = VisitDevice.BOT),
            VisitRecords.visit(at("2026-11-01T01:00:00Z"), device = VisitDevice.MOBILE),   // 7일 창 밖
        ))
        assertEquals(mapOf(VisitDevice.DESKTOP to 2L, VisitDevice.MOBILE to 0L, VisitDevice.BOT to 1L), store.summary(7, now).devices)
        assertEquals(mapOf(VisitDevice.DESKTOP to 2L, VisitDevice.MOBILE to 1L, VisitDevice.BOT to 1L), store.summary(30, now).devices)
    }

    @Test
    fun `지금 뒤의 줄은 요약에 들지 않는다`() {
        store.insertAll(listOf(VisitRecords.visit(now.toEpochMilli()), VisitRecords.visit(now.toEpochMilli() + 1)))
        assertEquals(1L, store.summary(7, now).daily.last().views)
    }

    @Test
    fun `요약에는 주소와 구분값이 없다`() {
        val day = at("2026-11-25T01:00:00Z")
        store.insertAll(listOf(VisitRecords.visit(day, 0, ip = ADDRESS_V4), VisitRecords.visit(day, 1, ip = ADDRESS_V6, visitor = VISITOR_TWO)))
        val text = store.summary(90, now).toString()
        for (secret in listOf(ADDRESS_V4, ADDRESS_V6, VISITOR_ONE, VISITOR_TWO)) assertFalse(text.contains(secret), "요약에 $secret 이 있다: $text")
    }
}
