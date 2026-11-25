package xyz.junproject.observe.visit

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.QueryTimeoutException
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.metric.FakeMetricStream
import java.time.Duration
import java.time.Instant

/**
 * 90일 보관 — 하루 한 번(기존 삭제와 같은 03:40 서울) `now − 90일` 보다 오래된 줄을 지운다. 경계는 시각(Instant) 기준이다 —
 * 기존 삭제(로그 30일 · 로그인 시도 90일)와 같다(서울의 날짜로 자르지 않는다). 지운 수는 로그와 헬스에, 실패는 종류 이름만.
 */
class VisitScheduleTest {
    private val now = Instant.parse("2026-11-24T18:40:00Z")      // 11-25 03:40 서울
    private val logs = CapturedLogs()

    @AfterEach fun detach() = logs.close()

    private class RecordingRetention : VisitRetention {
        val cutoffs = mutableListOf<Instant>()
        var failure: RuntimeException? = null
        override fun deleteOlderThan(cutoff: Instant): Int { failure?.let { throw it }; cutoffs += cutoff; return 1_234 }
    }

    private val retention = RecordingRetention()
    private val schedule = VisitSchedule(VisitCollector(FakeMetricStream(), FakeVisitSink(), block = Duration.ZERO), retention)

    @Test
    fun `지금에서 정확히 90일 앞을 기준으로 지우고 결과와 지운 수가 남는다`() {
        assertNull(schedule.lastRetention)
        assertEquals(1_234, schedule.deleteOldVisits(now))
        assertEquals(listOf(Instant.parse("2026-08-26T18:40:00Z")), retention.cutoffs)
        assertEquals(Duration.ofDays(90), Duration.between(retention.cutoffs.single(), now))
        assertEquals(RetentionResult(now, 1_234, null), schedule.lastRetention)
        assertTrue(logs.lines().any { it.contains("1234") && it.contains("90") }, "지운 수가 로그에 없다: ${logs.lines()}")
    }

    @Test
    fun `삭제가 실패하면 예외를 내지 않고 종류 이름만 남긴다`() {
        retention.failure = QueryTimeoutException("canceling statement — 내부 문구")
        assertNull(schedule.deleteOldVisits(now))
        assertEquals(RetentionResult(now, null, "QueryTimeoutException"), schedule.lastRetention)
        assertTrue(logs.lines().any { it.contains("QueryTimeoutException") }, "삭제 실패가 로그에 없다: ${logs.lines()}")
    }

    @Test
    fun `삭제는 매일 03시 40분 서울 시각에 예약되어 있다`() {
        val method = VisitSchedule::class.java.getMethod("deleteOldVisits")
        val scheduled = method.getAnnotation(Scheduled::class.java) ?: fail("deleteOldVisits() 에 @Scheduled 가 없다")
        assertEquals("Asia/Seoul", scheduled.zone)
        assertTrue(scheduled.cron.endsWith("0 40 3 * * *}") || scheduled.cron == "0 40 3 * * *", scheduled.cron)
    }
}
