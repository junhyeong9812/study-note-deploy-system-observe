package xyz.junproject.observe.visit

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.QueryTimeoutException
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.visit.VisitRecords.ADDRESS_V4
import xyz.junproject.observe.metric.FakeMetricStream
import java.time.Duration
import java.time.Instant

/**
 * 90일 보관 — 하루 한 번(기존 삭제와 같은 03:40 서울) `now − 90일` 보다 오래된 줄을 지운다. 경계는 시각(Instant) 기준이다 —
 * 기존 삭제(로그 30일 · 로그인 시도 90일)와 같다(서울의 날짜로 자르지 않는다). 지운 수는 로그와 헬스에, 실패는 종류 이름만.
 * 나눠 지운다: 한 문장에 묶음 크기(기본 5,000)까지, 한 문장이 묶음보다 적게 지우면 끝 · 문장이 끝날 때마다 시작부터 흐른 시간이
 * 시간 예산 이상이면 멈추고 남긴다(적어도 한 문장은 돈다) — 다음 실행이 잇는다. 기준 시각(`now − 90일`)은 한 번의 실행 안에서 같다.
 * 삭제 일정의 설정 값 `-`(끔)은 받지 않는다 — 주소를 저장하면서 삭제를 끌 수 없다.
 */
class VisitScheduleTest {
    private val now = Instant.parse("2026-11-24T18:40:00Z")      // 11-25 03:40 서울
    private val logs = CapturedLogs()

    @AfterEach fun detach() = logs.close()

    /**
     * 기한을 넘긴 줄 [overdueRows] 개를 가진 가짜 표 — 문장 하나가 많아야 limit 줄을 지우고, 문장마다 [statementTakes] 만큼 [clock] 을 민다.
     * 문장마다 받은 기준 시각 · 묶음 크기 · 지운 수를 남긴다.
     */
    private class RecordingRetention(var overdueRows: Int = 1_234, private val statementTakes: Duration = Duration.ofSeconds(1)) : VisitRetention {
        var clock: Instant = Instant.parse("2026-11-24T18:40:00Z")
        val cutoffs = mutableListOf<Instant>()
        val limits = mutableListOf<Int>()
        val deletedPerStatement = mutableListOf<Int>()
        var failure: RuntimeException? = null
        override fun deleteOlderThan(cutoff: Instant, limit: Int): Int {
            failure?.let { throw it }
            cutoffs += cutoff; limits += limit
            val deleted = minOf(limit, overdueRows)
            overdueRows -= deleted
            deletedPerStatement += deleted
            clock = clock.plus(statementTakes)
            return deleted
        }
    }

    private val retention = RecordingRetention()
    private val collector = VisitCollector(FakeMetricStream(), FakeVisitSink(), block = Duration.ZERO)
    private val schedule = VisitSchedule(collector, retention)

    private fun schedule(retention: RecordingRetention, batchSize: Int = 5_000, timeBudget: Duration = Duration.ofMinutes(5)) =
        VisitSchedule(collector, retention, retentionBatchSize = batchSize, retentionTimeBudget = timeBudget, clock = { retention.clock })

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
    fun `삭제가 실패하면 예외를 내지 않고 종류 이름만 남기며 예외의 글은 로그에 없다`() {
        retention.failure = QueryTimeoutException("canceling statement — 내부 문구 ('$ADDRESS_V4')")
        assertNull(schedule.deleteOldVisits(now))
        assertEquals(RetentionResult(now, null, "QueryTimeoutException"), schedule.lastRetention)
        assertTrue(logs.lines().any { it.contains("QueryTimeoutException") }, "삭제 실패가 로그에 없다: ${logs.lines()}")
        assertFalse(logs.text().contains(ADDRESS_V4), "삭제 실패의 로그에 주소가 나왔다: ${logs.text()}")
        assertFalse(logs.text().contains("내부 문구"), "삭제 실패의 로그에 예외의 글이 실렸다: ${logs.text()}")
    }

    // ── 나눠 지운다 ─────────────────────────────────────────

    @Test
    fun `묶음 크기보다 많이 밀렸으면 한 번의 실행에서 묶음씩 나눠 끝까지 지우고 한 문장은 묶음을 넘지 않는다`() {
        val backlog = RecordingRetention(overdueRows = 12_000)
        val schedule = schedule(backlog)
        assertEquals(12_000, schedule.deleteOldVisits(now))
        assertEquals(0, backlog.overdueRows, "기한을 넘긴 줄이 남았다")
        assertEquals(listOf(5_000, 5_000, 2_000), backlog.deletedPerStatement, "문장이 묶음씩 나뉘지 않았거나 묶음보다 적게 지운 뒤에도 이었다")
        assertEquals(listOf(5_000, 5_000, 5_000), backlog.limits)
        assertEquals(setOf(now.minus(Duration.ofDays(90))), backlog.cutoffs.toSet(), "한 번의 실행 안에서 기준 시각이 바뀌었다")
        assertEquals(RetentionResult(now, 12_000, null), schedule.lastRetention)
        assertTrue(logs.lines().any { it.contains("12000") }, "지운 수(합)가 로그에 없다: ${logs.lines()}")
    }

    @Test
    fun `묶음 크기의 배수만큼 밀렸으면 0 을 지운 문장에서 멈춘다`() {
        val backlog = RecordingRetention(overdueRows = 10_000)
        assertEquals(10_000, schedule(backlog).deleteOldVisits(now))
        assertEquals(listOf(5_000, 5_000, 0), backlog.deletedPerStatement)
    }

    @Test
    fun `밀린 것이 없으면 문장 하나로 끝난다`() {
        val nothing = RecordingRetention(overdueRows = 0)
        assertEquals(0, schedule(nothing).deleteOldVisits(now))
        assertEquals(listOf(0), nothing.deletedPerStatement)
    }

    @Test
    fun `시간 예산을 넘기면 멈추고 남기며 다음 실행이 이어서 지운다`() {
        // 문장 하나에 40초, 예산 60초 — 첫 문장 뒤 40초(예산 안) → 둘째 문장 뒤 80초(예산 넘김) → 멈춘다
        val slow = RecordingRetention(overdueRows = 12_000, statementTakes = Duration.ofSeconds(40))
        val schedule = schedule(slow, timeBudget = Duration.ofSeconds(60))
        assertEquals(10_000, schedule.deleteOldVisits(now))
        assertEquals(listOf(5_000, 5_000), slow.deletedPerStatement, "예산을 넘긴 뒤에도 이었다")
        assertEquals(2_000, slow.overdueRows)
        assertEquals(RetentionResult(now, 10_000, null), schedule.lastRetention)

        val nextDay = now.plus(Duration.ofDays(1))
        assertEquals(2_000, schedule.deleteOldVisits(nextDay))
        assertEquals(0, slow.overdueRows, "다음 실행이 남은 것을 잇지 않았다")
        assertEquals(RetentionResult(nextDay, 2_000, null), schedule.lastRetention)
    }

    @Test
    fun `예산이 정확히 다 찬 문장 뒤에는 멈추고 예산보다 짧으면 잇는다`() {
        val exact = RecordingRetention(overdueRows = 12_000, statementTakes = Duration.ofSeconds(30))
        assertEquals(10_000, schedule(exact, timeBudget = Duration.ofSeconds(60)).deleteOldVisits(now), "흐른 시간이 예산과 같으면 멈춘다")
        val under = RecordingRetention(overdueRows = 12_000, statementTakes = Duration.ofSeconds(29))
        assertEquals(12_000, schedule(under, timeBudget = Duration.ofSeconds(60)).deleteOldVisits(now))
    }

    @Test
    fun `예산을 이미 넘길 만큼 느린 문장이어도 한 문장은 지운다`() {
        val verySlow = RecordingRetention(overdueRows = 12_000, statementTakes = Duration.ofMinutes(10))
        assertEquals(5_000, schedule(verySlow, timeBudget = Duration.ofSeconds(1)).deleteOldVisits(now))
    }

    @Test
    fun `묶음 크기는 설정한 값을 쓴다`() {
        val backlog = RecordingRetention(overdueRows = 250)
        assertEquals(250, schedule(backlog, batchSize = 100).deleteOldVisits(now))
        assertEquals(listOf(100, 100, 50), backlog.deletedPerStatement)
        assertEquals(listOf(100, 100, 100), backlog.limits)
    }

    @Test
    fun `나눠 지우다 실패하면 예외를 내지 않고 실패로 남긴다`() {
        val failing = object : VisitRetention {
            var statements = 0
            override fun deleteOlderThan(cutoff: Instant, limit: Int): Int {
                statements++
                if (statements == 2) throw QueryTimeoutException("canceling statement ('$ADDRESS_V4')")
                return limit
            }
        }
        val schedule = VisitSchedule(collector, failing, retentionBatchSize = 5_000, retentionTimeBudget = Duration.ofMinutes(5))
        assertNull(schedule.deleteOldVisits(now))
        assertEquals(2, failing.statements, "실패한 뒤에도 이었다")
        assertEquals("QueryTimeoutException", schedule.lastRetention?.failure)
        assertFalse(logs.text().contains(ADDRESS_V4), logs.text())
    }

    // ── 설정 — 삭제를 끌 수 없다 ────────────────────────────

    private fun scheduleFromSettings(cron: String = "0 40 3 * * *", batchSize: Int = 5_000, timeBudget: String = "5m",
                                     retention: VisitRetention = RecordingRetention()) =
        ObserveConfiguration().visitSchedule(collector, retention = retention, retentionCron = cron, retentionBatchSize = batchSize, retentionTimeBudget = timeBudget)

    @Test
    fun `삭제 일정이 끔(-)이면 설정을 만들 때 실패하고 오류 글에 설정 이름과 까닭이 있다`() {
        val error = assertThrows(IllegalArgumentException::class.java) { scheduleFromSettings(cron = "-") }
        val message = error.message ?: ""
        assertTrue(message.contains("observe.visits.retention-cron"), "오류 글에 설정 이름이 없다: $message")
        assertTrue(message.contains("끌 수 없다"), "오류 글에 까닭이 없다: $message")
    }

    @Test
    fun `기본 일정과 정상 값이면 설정이 만들어진다`() {
        assertNotNull(scheduleFromSettings())
        assertNotNull(scheduleFromSettings(cron = "0 10 4 * * *", batchSize = 1, timeBudget = "30s"))
    }

    @Test
    fun `묶음 크기가 1 보다 작거나 시간 예산이 0 이하면 설정을 만들 때 실패하고 오류 글에 설정 이름이 있다`() {
        for (batchSize in listOf(0, -1)) {
            val error = assertThrows(IllegalArgumentException::class.java) { scheduleFromSettings(batchSize = batchSize) }
            assertTrue((error.message ?: "").contains("observe.visits.retention-batch-size"), "$batchSize: ${error.message}")
        }
        for (timeBudget in listOf("0s", "-1m")) {
            val error = assertThrows(IllegalArgumentException::class.java) { scheduleFromSettings(timeBudget = timeBudget) }
            assertTrue((error.message ?: "").contains("observe.visits.retention-time-budget"), "$timeBudget: ${error.message}")
        }
    }

    @Test
    fun `설정한 묶음 크기가 설정에서 만든 일정의 삭제에 쓰인다`() {
        val backlog = RecordingRetention(overdueRows = 250)
        assertEquals(250, scheduleFromSettings(batchSize = 100, retention = backlog).deleteOldVisits(now))
        assertEquals(listOf(100, 100, 100), backlog.limits, "설정의 묶음 크기가 일정에 닿지 않았다")
    }

    @Test
    fun `삭제는 매일 03시 40분 서울 시각에 예약되어 있다`() {
        val method = VisitSchedule::class.java.getMethod("deleteOldVisits")
        val scheduled = method.getAnnotation(Scheduled::class.java) ?: fail("deleteOldVisits() 에 @Scheduled 가 없다")
        assertEquals("Asia/Seoul", scheduled.zone)
        assertTrue(scheduled.cron.endsWith("0 40 3 * * *}") || scheduled.cron == "0 40 3 * * *", scheduled.cron)
    }
}
