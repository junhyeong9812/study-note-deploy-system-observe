package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.QueryTimeoutException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.AbstractDataSource
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.auth.AuthAttemptPolicy
import xyz.junproject.observe.auth.AuthAttemptQuery
import xyz.junproject.observe.auth.AuthAttemptRequest
import xyz.junproject.observe.auth.AuthAttemptRow
import xyz.junproject.observe.auth.AuthAttempts
import xyz.junproject.observe.auth.AuthLockStatus
import xyz.junproject.observe.auth.AuthOutcome
import xyz.junproject.observe.auth.AuthRejectedAttempt
import xyz.junproject.observe.auth.AuthVerdict
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.RetentionResult
import java.sql.Connection
import java.sql.SQLException
import java.time.Instant

/**
 * 명세 §2.3 보관 · §11.3 — 지표의 7일·30일 삭제는 기존 예약 실행(매일 03:40)에서 로그 30일 · 로그인 시도 90일 삭제와 **서로 따로** 돈다
 * (한쪽이 실패해도 나머지는 돈다). 5분 평균은 따로 예약되어 돌고 결과(성공 시각 · 실패 사유)를 남긴다. 시각은 인자로 준다.
 */
class MetricScheduleTest {
    private val now = Instant.parse("2026-11-24T18:40:00Z")

    private class UnreachableDataSource : AbstractDataSource() {
        override fun getConnection(): Connection = throw SQLException("Connection refused — 내부 문구")
        override fun getConnection(username: String?, password: String?): Connection = getConnection()
    }

    private class CountingMaintenance : MetricMaintenance {
        var rollUpFailure: RuntimeException? = null
        var deleteFailure: RuntimeException? = null
        val rollUps = mutableListOf<Instant>()
        val deletions = mutableListOf<Instant>()
        override fun rollUp(now: Instant): Int { rollUpFailure?.let { throw it }; rollUps += now; return 4 }
        override fun deleteExpired(now: Instant): MetricDeletion { deleteFailure?.let { throw it }; deletions += now; return MetricDeletion(5_760, 288) }
    }

    private class CountingAuthAttempts : AuthAttempts {
        var deleteFailure: RuntimeException? = null
        val calls = mutableListOf<String>()
        override fun createSchema() {}
        override fun decide(request: AuthAttemptRequest) = AuthVerdict(true, AuthOutcome.SUCCESS, null, false, 0)
        override fun recordRejected(attempt: AuthRejectedAttempt) {}
        override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> = emptyList()
        override fun lockStatus(now: Instant) = AuthLockStatus(false, null, 0)
        override fun deleteOlderThan(cutoff: Instant): Int { deleteFailure?.let { throw it }; calls += "delete"; return 3 }
    }

    private val unreachableLogStore = JdbcTemplate(UnreachableDataSource()).let { LogStore(it, it, it) }

    private fun schedule(maintenance: MetricMaintenance?, attempts: AuthAttempts = CountingAuthAttempts(), logStore: LogStore = unreachableLogStore) =
        ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), logStore, attempts, AuthAttemptPolicy(), metrics = maintenance)

    @Test
    fun `지표 삭제는 지금 시각으로 돌고 결과(시각·건수)가 남는다`() {
        val maintenance = CountingMaintenance()
        val schedule = schedule(maintenance)
        assertNull(schedule.lastMetricRetention)
        assertEquals(MetricDeletion(5_760, 288), schedule.deleteOldMetrics(now))
        assertEquals(listOf(now), maintenance.deletions)
        assertEquals(MetricRetentionResult(now, MetricDeletion(5_760, 288), null), schedule.lastMetricRetention)
    }

    @Test
    fun `지표 삭제가 실패해도 로그 삭제와 로그인 시도 삭제는 돌고 각자의 결과가 남는다`() {
        val maintenance = CountingMaintenance().apply { deleteFailure = QueryTimeoutException("내부 문구") }
        val attempts = CountingAuthAttempts()
        val schedule = schedule(maintenance, attempts)
        schedule.deleteOldLogs()                                       // 예약이 부르는 그 함수
        assertNotNull(schedule.lastRetention, "로그 삭제가 돌지 않았다")
        assertEquals(listOf("delete"), attempts.calls, "지표 삭제 실패가 로그인 시도 삭제를 막았다")
        val metricResult = schedule.lastMetricRetention ?: fail("예약 실행이 지표 삭제를 돌지 않았다")
        assertNull(metricResult.deletion)
        assertEquals("QueryTimeoutException", metricResult.failure)
    }

    @Test
    fun `로그 삭제와 로그인 시도 삭제가 실패해도 지표 삭제는 돈다`() {
        val maintenance = CountingMaintenance()
        val attempts = CountingAuthAttempts().apply { deleteFailure = QueryTimeoutException("내부 문구") }
        val schedule = schedule(maintenance, attempts, unreachableLogStore)
        schedule.deleteOldLogs()
        assertEquals("CannotGetJdbcConnectionException", schedule.lastRetention?.failure)
        assertEquals("QueryTimeoutException", schedule.lastAuthRetention?.failure)
        assertEquals(1, maintenance.deletions.size, "로그 삭제 실패가 지표 삭제를 막았다")
        assertNull(schedule.lastMetricRetention?.failure)
        assertEquals(MetricDeletion(5_760, 288), schedule.lastMetricRetention?.deletion)
    }

    @Test
    fun `지표 저장소가 없으면 지표 삭제를 건너뛰고 기존 두 삭제는 그대로다`() {
        val attempts = CountingAuthAttempts()
        val schedule = schedule(null, attempts)
        schedule.deleteOldLogs()
        assertNull(schedule.deleteOldMetrics(now))
        assertNull(schedule.lastMetricRetention)
        assertEquals(listOf("delete"), attempts.calls)
        assertEquals(RetentionResult(schedule.lastAuthRetention!!.ranAt, 3, null), schedule.lastAuthRetention)
    }

    @Test
    fun `5분 평균 바퀴의 성공은 시각과 구간 수를, 실패는 종류 이름만 남기고 마지막 성공 시각은 그대로다`() {
        val maintenance = CountingMaintenance()
        val schedule = schedule(maintenance)
        assertNull(schedule.lastMetricRollup)
        assertNull(schedule.lastMetricRollupSuccessAt)
        assertEquals(4, schedule.rollUpMetrics(now))
        assertEquals(listOf(now), maintenance.rollUps)
        assertEquals(MetricRollupResult(now, 4, null), schedule.lastMetricRollup)
        assertEquals(now, schedule.lastMetricRollupSuccessAt)

        maintenance.rollUpFailure = QueryTimeoutException("canceling statement — 내부 문구")
        val later = now.plusSeconds(60)
        assertNull(schedule.rollUpMetrics(later), "실패가 예외로 새어 나왔거나 값이 나왔다")
        assertEquals(MetricRollupResult(later, null, "QueryTimeoutException"), schedule.lastMetricRollup)
        assertEquals(now, schedule.lastMetricRollupSuccessAt, "실패한 바퀴가 마지막 성공 시각을 바꿨다")
    }

    @Test
    fun `5분 평균은 설정 키의 간격으로 예약되어 있다`() {
        val method = ObserveSchedule::class.java.getMethod("rollUpMetrics")
        val scheduled = method.getAnnotation(Scheduled::class.java) ?: fail("rollUpMetrics() 에 @Scheduled 가 없다")
        assertEquals("\${observe.metrics.rollup-interval-ms:60000}", scheduled.fixedDelayString)
    }
}
