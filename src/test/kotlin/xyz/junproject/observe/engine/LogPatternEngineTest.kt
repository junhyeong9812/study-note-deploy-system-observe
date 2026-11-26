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
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.logalert.LogAlertStore
import xyz.junproject.observe.logalert.LogPattern
import xyz.junproject.observe.logalert.LogPatternJudge
import xyz.junproject.observe.logalert.LogPatternRound
import xyz.junproject.observe.logalert.LogPatternSettings
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.AlertSeverity
import xyz.junproject.observe.threshold.LogPatternRule
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Duration
import java.time.Instant

/**
 * 실제 PostgreSQL — ⑪-6a 글자 규칙: 로그 표(`log_entry`)의 줄을 글자 **그대로**(대소문자 구분 · 정규식 · `LIKE` 의 특수 글자도 글자로) 맞추고,
 * 사건은 무수신 알림과 같은 보낼 표로 들어 지금의 보내는 곳([AlertDispatcher])이 보낸다(모르는 규칙으로 건너뛰지 않는다) · 다시 보내기의 규칙도 같다 ·
 * 두 자리가 같은 때 판정해도 사건은 한 건(판정 잠금과 상태 표) · 5분 넘게 오래된 줄은 판정하지 않는다 · 보낼 표 어디에도 로그의 내용이 없다.
 * 표는 전용 스키마(`log_pattern`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogPatternEngineTest {
    private val schema = "log_pattern"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private fun place() = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@LogPatternEngineTest.schema},public"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val dataSource = place()
    private val otherPlace = place()
    private val jdbc = JdbcTemplate(dataSource)
    private val logStore = EngineSupport.store(dataSource)
    private val alertStore = LogAlertStore(jdbc)
    private val outbox = ThresholdStore(jdbc)
    private val now = Instant.parse("2026-11-29T03:00:00Z")
    private var sequence = 0

    private val limit = LogPattern("claude_limit", "backend", AlertSeverity.CRITICAL, "result=limit_reached")

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        logStore.createSchema()
        alertStore.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close(); otherPlace.close(); administration.close()
    }

    private fun line(at: Instant, message: String, service: String = "backend", requestId: String = "req-${++sequence}") =
        logStore.insertAll(listOf(LogEntry(StreamId(at.toEpochMilli(), sequence), "info", service, requestId, message)))

    private fun round(settings: LogPatternSettings = LogPatternSettings(listOf(limit), emptyList()), store: LogAlertStore = alertStore) =
        LogPatternRound(settings, store, store, LogPatternJudge(settings)) { true }

    private fun outboxRows(): List<Map<String, Any?>> = jdbc.queryForList("SELECT * FROM resource_alert_outbox ORDER BY id")

    @Test
    fun `맞춤은 서비스가 같고 내용에 글자가 그대로 들었을 때뿐 — 대소문자 · 정규식 · LIKE 의 글자도 글자 그대로`() {
        val at = now.minusSeconds(60)
        line(at, "claude run: result=limit_reached auto=false duration_ms=5")
        line(at.plusMillis(1), "CLAUDE RUN: RESULT=LIMIT_REACHED")
        line(at.plusMillis(2), "claude run: result=limit-reached")
        line(at.plusMillis(3), "claude run: result=limit_reached", service = "front")
        line(at.plusMillis(4), "pattern x a.c*( y")
        line(at.plusMillis(5), "pattern x abcc( y")
        line(at.plusMillis(6), "done 100%_ok")
        line(at.plusMillis(7), "done 100xxok")
        fun count(text: String, service: String = "backend") = alertStore.matches(service, text, at.minusSeconds(1), now).count
        assertEquals(1, count("result=limit_reached"), "대소문자 · 비슷한 글자 · 다른 서비스를 맞췄다")
        assertEquals(1, count("result=limit_reached", service = "front"))
        assertEquals(1, count("a.c*("), "정규식으로 맞췄다")
        assertEquals(1, count("100%_ok"), "LIKE 의 % · _ 를 특수 글자로 맞췄다")
        assertEquals(0, count("nothing like this"))
    }

    @Test
    fun `한 바퀴의 사건은 보낼 표에 정한 꼴로 들고 로그의 내용은 어디에도 없다 · 첫 줄의 요청 ID 와 시각이 실린다`() {
        line(now.minusSeconds(50), "claude run: result=limit_reached question=MARKER-private-text", requestId = "req-first-one")
        line(now.minusSeconds(40), "claude run: result=limit_reached question=MARKER-private-text")
        round().judgeOnce(now)
        val row = outboxRows().single()
        assertEquals("log_pattern", row["kind"])
        assertEquals("log_pattern:claude_limit", row["alert_key"])
        assertEquals("critical", row["severity"])
        assertEquals(LogPatternRule.PATTERN.wireName, row["rule"])
        assertEquals("2줄 맞음 · 첫 줄 요청 ID req-first-one · 시각 ${now.minusSeconds(50)}", row["detail"])
        assertFalse(row.values.any { it?.toString()?.contains("MARKER") == true }, "보낼 표에 로그의 내용이 실렸다: $row")
    }

    @Test
    fun `5분 넘게 오래된 줄 — 다시 뜬 뒤의 밀린 묶음 — 은 판정하지 않는다`() {
        line(now.minus(Duration.ofMinutes(5)).minusSeconds(1), "claude run: result=limit_reached")
        round().judgeOnce(now)
        assertEquals(emptyList<Map<String, Any?>>(), outboxRows())
    }

    @Test
    fun `10분에 한 번 — 창 안의 맞춤은 쌓였다가 창이 지난 뒤의 바퀴에 한 건으로 나간다`() {
        line(now.minusSeconds(40), "result=limit_reached", requestId = "req-start")
        val round = round()
        round.judgeOnce(now)
        line(now.plusSeconds(30), "result=limit_reached", requestId = "req-later")
        line(now.plusSeconds(60), "result=limit_reached")
        round.judgeOnce(now.plusSeconds(90))
        assertEquals(1, outboxRows().size, "창 안에 또 보냈다")
        // 그사이의 바퀴 — 운영처럼 30초마다(판정이 멈춘 것을 흉내 내지 않게)
        for (at in generateSequence(now.plusSeconds(120)) { it.plusSeconds(30) }.takeWhile { it.isBefore(now.plus(Duration.ofMinutes(10))) }) round.judgeOnce(at)
        assertEquals(1, outboxRows().size, "창 안에 또 보냈다")
        round.judgeOnce(now.plus(Duration.ofMinutes(10)))
        assertEquals(listOf("1줄 맞음 · 첫 줄 요청 ID req-start · 시각 ${now.minusSeconds(40)}", "2줄 맞음 · 첫 줄 요청 ID req-later · 시각 ${now.plusSeconds(30)}"),
            outboxRows().map { it["detail"] })
    }

    @Test
    fun `규칙을 뺐다가 한참 뒤 같은 이름으로 다시 넣어도 옛 맞춤으로 사건을 내지 않고 그 뒤 새 줄만으로 한 건을 낸다`() {
        line(now.minusSeconds(40), "result=limit_reached", requestId = "req-revive-1")
        round().judgeOnce(now)                                                          // 첫 사건
        line(now.plusSeconds(10), "result=limit_reached", requestId = "req-revive-2")
        round().judgeOnce(now.plusSeconds(30))                                          // 창 안 — 쌓인 수 1
        round(settings = LogPatternSettings(emptyList(), emptyList())).judgeOnce(now.plusSeconds(60))   // 규칙을 뺀 설정의 바퀴 — 상태는 남는다
        assertEquals(1, outboxRows().size)
        val later = now.plus(Duration.ofDays(7))
        round().judgeOnce(later)                                                        // 다시 넣은 첫 바퀴 — 새 줄 없음
        assertEquals(1, outboxRows().size, "다시 넣은 첫 바퀴에 7일 전의 맞춤으로 사건을 냈다: ${outboxRows().map { it["detail"] }}")
        line(later.plusSeconds(5), "result=limit_reached", requestId = "req-revive-3")
        round().judgeOnce(later.plusSeconds(30))
        assertEquals(listOf("1줄 맞음 · 첫 줄 요청 ID req-revive-3 · 시각 ${later.plusSeconds(5)}"), outboxRows().drop(1).map { it["detail"] })
    }

    @Test
    fun `두 자리가 같은 때 판정해도 사건은 한 건이다`() {
        line(now.minusSeconds(40), "result=limit_reached")
        round().judgeOnce(now)
        round(store = LogAlertStore(JdbcTemplate(otherPlace))).judgeOnce(now.plusSeconds(1))
        assertEquals(1, outboxRows().size)
    }

    private class RecordingSender(var outcome: SendOutcome = SendOutcome.Delivered) : AlertSender {
        val sent = mutableListOf<ResourceAlert>()
        override fun send(alert: ResourceAlert): SendOutcome { sent += alert; return outcome }
    }

    @Test
    fun `지금의 보내는 곳이 글자 규칙의 사건을 보내고 알림 서버가 죽어도 다시 보내며 그동안 로그는 그대로 쌓인다`() {
        line(now.minusSeconds(40), "result=limit_reached")
        round().judgeOnce(now)
        val sender = RecordingSender(SendOutcome.Retry("http 503"))
        val dispatcher = AlertDispatcher(outbox, sender)
        dispatcher.dispatch(now.plusSeconds(1))
        assertEquals(listOf("log_pattern"), sender.sent.map { it.kind }, "보내기가 글자 규칙의 사건을 모르는 규칙으로 건너뛰었다")
        assertEquals(1L, outbox.alertCounts().undelivered, "다시 보낼 것으로 남지 않았다")
        line(now.plusSeconds(2), "다른 줄 — 알림 서버가 죽은 동안에도 저장된다")
        assertEquals(1, logStore.search(xyz.junproject.observe.log.LogQuery(now, now.plusSeconds(3), limit = 10)).size)
        sender.outcome = SendOutcome.Delivered
        dispatcher.dispatch(now.plusSeconds(60))
        assertEquals(0L, outbox.alertCounts().undelivered)
        assertEquals(2, sender.sent.size)
    }
}
