package xyz.junproject.observe.logalert

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.dao.QueryTimeoutException
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.threshold.RoundOutcome
import java.time.Duration
import java.time.Instant

/**
 * 무수신 판정 한 바퀴와 헬스 칸: 설정이 꺼져 있으면 아무것도 읽지 않는다 · 잠금을 얻은 바퀴 안에서 서비스마다의 마지막 로그 · 전체의 마지막 로그 · 끊긴 서버를 읽어 판정한다 ·
 * 실패는 예외로 새지 않고 결과와 헬스에 종류 이름으로 남으며 ERROR 한 줄(종류 이름만 — 예외의 문구 · 주소는 싣지 않는다) · 다음 바퀴는 다시 돈다 ·
 * 헬스의 `log_alerts` 칸 = `{enabled, watched_services, watch_all, judging, last_judged_at, last_failure}`(시각 · 실패가 없으면 빈 글자) ·
 * 수집 헬스는 그 칸을 본문에 싣기만 하고 `status` · HTTP 상태는 바뀌지 않는다.
 */
class LogAlertRoundTest {
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val settings = LogAlertSettings(listOf(WatchedService("backend", "ci-agent-158", Duration.ofMinutes(30)),
        WatchedService("front", "ci-agent-9", Duration.ofMinutes(45))), null, emptyList())

    private class Store : LogRuleStateStore {
        var calls = 0
        var failure: RuntimeException? = null
        var lockAvailable = true
        var states: List<LogRuleState> = emptyList()
        override fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome {
            calls++
            failure?.let { throw it }
            if (!lockAvailable) return RoundOutcome(false, 0)
            val result = judge(states)
            states = result.states
            return RoundOutcome(true, result.events.size)
        }
    }

    private class Receipts : LogReceipts {
        val asked = mutableListOf<Collection<String>>()
        var last: Map<String, Instant?> = emptyMap()
        var silent: Set<String> = emptySet()
        var failure: RuntimeException? = null
        override fun lastReceived(services: Collection<String>): Map<String, Instant?> { failure?.let { throw it }; asked += services; return services.associateWith { last[it] } }
        override fun lastReceivedAny(): Instant? = last.values.filterNotNull().maxOrNull()
        override fun silentServers(): Set<String> = silent
    }

    private val store = Store()
    private val receipts = Receipts()
    private var judging = true
    private fun round(with: LogAlertSettings = settings) = LogAlertRound(with, store, receipts, LogSilentJudge(with)) { judging }

    private val appender = ListAppender<ILoggingEvent>().also { it.start(); (LoggerFactory.getLogger("observe") as Logger).addAppender(it) }
    @AfterEach fun detach() { (LoggerFactory.getLogger("observe") as Logger).detachAppender(appender) }

    @Test
    fun `설정이 꺼져 있으면 판정하지 않고 아무것도 읽지 않으며 헬스에 꺼짐이 보인다`() {
        val off = round(LogAlertSettings(emptyList(), null, emptyList()))
        val result = off.judgeOnce(now)
        assertFalse(result.judged)
        assertNull(result.failure)
        assertEquals(0, store.calls)
        assertEquals(emptyList<Collection<String>>(), receipts.asked)
        assertEquals(mapOf("enabled" to false, "watched_services" to 0, "watch_all" to false, "judging" to true, "last_judged_at" to "", "last_failure" to ""),
            off.health(now))
    }

    @Test
    fun `잠금을 얻은 바퀴는 감시하는 서비스의 마지막 로그를 읽어 판정하고 끊긴 서버를 판정에 넘긴다`() {
        receipts.last = mapOf("backend" to now.minus(Duration.ofHours(1)), "front" to now)
        receipts.silent = setOf("ci-agent-9")
        val oneRound = round()
        val result = oneRound.judgeOnce(now)
        assertEquals(LogAlertRoundResult(now, true, 1, null), result)
        assertEquals(listOf(setOf("backend", "front")), receipts.asked.map { it.toSet() })
        assertTrue(store.states.single { it.service == "backend" }.firing)
        assertEquals(now, oneRound.lastJudgedAt)
        assertEquals(mapOf("enabled" to true, "watched_services" to 2, "watch_all" to false, "judging" to true, "last_judged_at" to now.toString(), "last_failure" to ""),
            oneRound.health(now))
    }

    @Test
    fun `보류 중이면 판정에 보류를 넘겨 알림을 만들지 않고 헬스에 판정하지 않음이 보인다`() {
        judging = false
        receipts.last = mapOf("backend" to null, "front" to null)
        val oneRound = round()
        assertEquals(0, oneRound.judgeOnce(now).eventsCreated)
        assertEquals(false, oneRound.health(now)["judging"])
    }

    @Test
    fun `잠금을 얻지 못한 바퀴는 판정 시각을 바꾸지 않고 실패도 아니다`() {
        store.lockAvailable = false
        val oneRound = round()
        assertEquals(LogAlertRoundResult(now, false, 0, null), oneRound.judgeOnce(now))
        assertNull(oneRound.lastJudgedAt)
    }

    @Test
    fun `판정이 실패하면 예외가 새지 않고 종류 이름이 결과 · 헬스 · ERROR 한 줄에 남으며 문구는 싣지 않고 다음 바퀴는 다시 돈다`() {
        val oneRound = round()
        store.failure = QueryTimeoutException("canceling statement on jdbc:postgresql://observe-db:5432/observe")
        val failed = assertDoesNotThrow<LogAlertRoundResult> { oneRound.judgeOnce(now) }
        assertEquals(LogAlertRoundResult(now, false, 0, "QueryTimeoutException"), failed)
        assertEquals("QueryTimeoutException", oneRound.health(now)["last_failure"])
        val errors = appender.list.filter { it.level == Level.ERROR }
        assertEquals(1, errors.size, appender.list.map { it.formattedMessage }.toString())
        assertTrue(errors.single().formattedMessage.contains("QueryTimeoutException"))
        assertFalse(errors.single().formattedMessage.contains("jdbc:postgresql") || errors.single().formattedMessage.contains("observe-db"), "ERROR 에 예외의 문구가 실렸다")
        assertNull(errors.single().throwableProxy)

        store.failure = null
        receipts.failure = IllegalStateException("log_entry 읽기 실패 password=do-not-log")
        assertEquals("IllegalStateException", oneRound.judgeOnce(now.plusSeconds(30)).failure, "읽기의 실패가 결과에 남지 않았다")
        receipts.failure = null
        val recovered = oneRound.judgeOnce(now.plusSeconds(60))
        assertTrue(recovered.judged, "실패 뒤 다음 바퀴가 돌지 않았다")
        assertNull(recovered.failure)
        assertEquals("", oneRound.health(now)["last_failure"], "성공한 바퀴 뒤에도 실패가 남았다")
        assertEquals(3, store.calls)
    }

    @Test
    fun `수집 헬스는 log_alerts 칸을 싣기만 하고 판정이 실패해도 status 와 HTTP 상태는 그대로다`() {
        val stream = FakeLogStream()
        val collector = LogCollector(stream, FakeLogSink(), block = Duration.ZERO, clock = { now }).also { it.runOnce() }
        val oneRound = round()
        store.failure = IllegalStateException("판정 실패")
        oneRound.judgeOnce(now)
        val without = CollectorHealth(collector, { 0L }, { 0L }, { now }, { null }, startedAt = now).report(now)
        val with = CollectorHealth(collector, { 0L }, { 0L }, { now }, { null }, startedAt = now, logAlerts = oneRound).report(now)
        assertEquals(without.healthy, with.healthy)
        assertTrue(with.healthy)
        assertEquals("ok", with.body["status"], "판정의 실패가 수집 헬스의 상태를 바꿨다")
        assertEquals(oneRound.health(now), with.body["log_alerts"])
        assertEquals(without.body, with.body - "log_alerts", "로그 무수신 칸 말고 다른 칸이 바뀌었다")
        assertFalse(without.body.containsKey("log_alerts"), "판정을 붙이지 않았는데 칸이 있다")
    }
}
