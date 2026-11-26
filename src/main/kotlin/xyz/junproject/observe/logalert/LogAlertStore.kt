package xyz.junproject.observe.logalert

import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import xyz.junproject.observe.threshold.LogRule
import xyz.junproject.observe.threshold.ResourceRule
import xyz.junproject.observe.threshold.RoundOutcome
import xyz.junproject.observe.threshold.ThresholdStore
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.time.Duration
import java.time.Instant

/** 로그 규칙의 상태 저장 — 한 바퀴가 잠금 · 상태 · 보낼 알림을 한 트랜잭션에 */
interface LogRuleStateStore {
    fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome
}

/** 판정이 읽는 것 — 서비스마다의 마지막 로그 · 전체의 마지막 로그 · 자원 끊김이 넘어 있는 서버 */
interface LogReceipts {
    fun lastReceived(services: Collection<String>): Map<String, Instant?>
    fun lastReceivedAny(): Instant?
    fun silentServers(): Set<String>
}

/**
 * PostgreSQL 저장소(`log_rule_state` — 열쇠 = 서비스 · 규칙, 자원 판정의 상태 표와 따로) — 판정 · 알림 풀을 쓴다(새 풀을 만들지 않는다).
 * 보낼 알림은 자원 알림과 같은 보낼 표에([ThresholdStore.insertAlerts] — 같은 고유 키는 한 건).
 * [judgeRound] 는 판정 잠금(자원 판정과 **다른** 권고 잠금 — 기다리지 않고 시도)을 얻은 자리만 한 트랜잭션에서 앞 상태를 읽어 판정하고 상태 전부와 알림을 기록한다.
 * 그 트랜잭션의 조회(판정이 그 안에서 읽는 마지막 로그 포함)는 [readTimeout] 에서 끊긴다 — 상한은 그 트랜잭션에만 걸려(`set_config(…, true)`) 연결에 남지 않는다.
 * 마지막 로그는 로그 표의 `stream_ms`(Redis 가 받은 시각)에서 서비스 색인으로 읽는다.
 */
class LogAlertStore(private val jdbc: JdbcTemplate, private val readTimeout: Duration = Duration.ofSeconds(2)) : LogRuleStateStore, LogReceipts,
    LogPatternStateStore, LogPatternLines {
    private val dataSource = requireNotNull(jdbc.dataSource) { "JdbcTemplate 에 DataSource 가 없다" }
    private val transaction = TransactionTemplate(DataSourceTransactionManager(dataSource))
    private val outbox = ThresholdStore(jdbc)
    @Volatile private var schemaReady = false
    private val creatingSchema = Any()

    /** 상태 표(무수신 `log_rule_state` · 글자 규칙 `log_pattern_state`)와 보낼 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema() {
        outbox.createSchema()
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS log_rule_state (
                 service          text    NOT NULL,
                 rule             text    NOT NULL,
                 firing           boolean NOT NULL,
                 since_ms         bigint,
                 last_notified_ms bigint,
                 last_received_ms bigint,
                 PRIMARY KEY (service, rule))"""
        )
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS log_pattern_state (
                 name                     text   PRIMARY KEY,
                 checked_until_ms         bigint NOT NULL,
                 last_notified_ms         bigint,
                 pending_count            bigint NOT NULL,
                 pending_first_request_id text,
                 pending_first_ms         bigint)"""
        )
        schemaReady = true
    }

    private fun ensureSchema() {
        if (!schemaReady) synchronized(creatingSchema) { if (!schemaReady) createSchema() }
    }

    /** 뜬 직후 한 번 미리 만든다 — 실패는 예외로(부르는 쪽이 로그만 남기고 삼킨다) */
    fun prepareSchema() = ensureSchema()

    /** 저장된 상태 전부(테스트 · 확인용) */
    fun states(): List<LogRuleState> {
        ensureSchema()
        return jdbc.query("SELECT * FROM log_rule_state ORDER BY service, rule", { row, _ -> row.toState() })
    }

    override fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome {
        ensureSchema()
        return limited {
            val locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean::class.java, LOG_JUDGE_LOCK)!!
            if (!locked) return@limited RoundOutcome(false, 0)
            val previous = jdbc.query("SELECT * FROM log_rule_state ORDER BY service, rule", { row, _ -> row.toState() })
            val result = judge(previous)
            jdbc.update("DELETE FROM log_rule_state")
            if (result.states.isNotEmpty()) jdbc.batchUpdate(
                "INSERT INTO log_rule_state (service, rule, firing, since_ms, last_notified_ms, last_received_ms) VALUES (?,?,?,?,?,?)",
                result.states, result.states.size,
            ) { statement, state ->
                statement.setString(1, state.service); statement.setString(2, state.rule.wireName); statement.setBoolean(3, state.firing)
                statement.setObject(4, state.since?.toEpochMilli(), Types.BIGINT); statement.setObject(5, state.lastNotifiedAt?.toEpochMilli(), Types.BIGINT)
                statement.setObject(6, state.lastReceivedAt?.toEpochMilli(), Types.BIGINT)
            }
            RoundOutcome(true, outbox.insertAlerts(result.events))
        }
    }

    /**
     * 글자 규칙 판정 한 바퀴 — 무수신과 **다른** 권고 잠금([LOG_PATTERN_LOCK])을 기다리지 않고 시도해 얻은 자리만, 한 트랜잭션에서 앞 상태(`log_pattern_state`)를 읽어
     * 판정하고(판정이 그 안에서 로그 표를 읽는다 — [readTimeout] 에서 끊긴다) 상태 전부와 알림([ThresholdStore.insertAlerts])을 기록한다.
     * 판정은 자기 설정에 없는 앞 상태도 그대로 돌려주므로 다른 자리의 상태를 지우지 않는다
     */
    override fun judgePatternRound(now: Instant, judge: (previous: List<LogPatternState>) -> LogPatternJudgeResult): RoundOutcome {
        ensureSchema()
        return limited {
            val locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean::class.java, LOG_PATTERN_LOCK)!!
            if (!locked) return@limited RoundOutcome(false, 0)
            val previous = jdbc.query("SELECT * FROM log_pattern_state ORDER BY name", { row, _ -> row.toPatternState() })
            val result = judge(previous)
            jdbc.update("DELETE FROM log_pattern_state")
            if (result.states.isNotEmpty()) jdbc.batchUpdate(
                """INSERT INTO log_pattern_state (name, checked_until_ms, last_notified_ms, pending_count, pending_first_request_id, pending_first_ms)
                   VALUES (?,?,?,?,?,?)""",
                result.states, result.states.size,
            ) { statement, state ->
                statement.setString(1, state.name); statement.setLong(2, state.checkedUntil.toEpochMilli())
                statement.setObject(3, state.lastNotifiedAt?.toEpochMilli(), Types.BIGINT); statement.setLong(4, state.pendingCount)
                statement.setObject(5, state.pendingFirstRequestId, Types.VARCHAR); statement.setObject(6, state.pendingFirstAt?.toEpochMilli(), Types.BIGINT)
            }
            RoundOutcome(true, outbox.insertAlerts(result.events))
        }
    }

    /**
     * 로그 표에서 이 서비스의 줄 가운데 `stream_ms` 가 `(after, until]` 이고 내용에 [text] 가 글자 그대로 든 줄 — `strpos`(대소문자 구분 · 정규식 · LIKE 아님 —
     * `%` `_` 도 글자 그대로). 서비스 색인(`service, stream_ms`)으로 기간을 좁힌 뒤 견준다. 질의 둘: 수 · (맞은 줄이 있으면) 스트림 차례로 첫 줄
     */
    override fun matches(service: String, text: String, after: Instant, until: Instant): LogPatternMatches {
        val range = arrayOf<Any>(service, after.toEpochMilli(), until.toEpochMilli(), text)
        val count = jdbc.queryForObject(
            "SELECT count(*) FROM log_entry WHERE service = ? AND stream_ms > ? AND stream_ms <= ? AND strpos(message, ?) > 0", Long::class.java, *range)!!
        if (count == 0L) return LogPatternMatches(0, null, null)
        val first = jdbc.query(
            """SELECT request_id, stream_ms FROM log_entry WHERE service = ? AND stream_ms > ? AND stream_ms <= ? AND strpos(message, ?) > 0
               ORDER BY stream_ms, stream_seq LIMIT 1""",
            { row, _ -> row.getString("request_id") to Instant.ofEpochMilli(row.getLong("stream_ms")) }, *range,
        ).firstOrNull()
        return LogPatternMatches(count, first?.first, first?.second)
    }

    /** 서비스마다 마지막으로 받은 로그의 시각 — 이름이 정확히 같은 줄에서(서비스 색인의 첫 줄), 없으면 null */
    override fun lastReceived(services: Collection<String>): Map<String, Instant?> = services.associateWith { service ->
        jdbc.query("SELECT stream_ms FROM log_entry WHERE service = ? ORDER BY stream_ms DESC LIMIT 1", { row, _ -> row.getLong(1) }, service)
            .firstOrNull()?.let(Instant::ofEpochMilli)
    }

    /** 어느 서비스든 마지막으로 받은 로그의 시각 — 없으면 null */
    override fun lastReceivedAny(): Instant? =
        jdbc.query("SELECT stream_ms FROM log_entry ORDER BY stream_ms DESC, stream_seq DESC LIMIT 1", { row, _ -> row.getLong(1) })
            .firstOrNull()?.let(Instant::ofEpochMilli)

    /**
     * 자원 끊김이 넘어 있는 서버(`resource_rule_state` 의 끊김 규칙 · 넘음) — 판정의 잠금 트랜잭션 **밖에서** 읽는다(그 안에서 표가 없으면 트랜잭션 전체가 망가진다).
     * 자원 판정의 표가 아직 없으면 빈 집합
     */
    override fun silentServers(): Set<String> = try {
        limited {
            jdbc.queryForList("SELECT DISTINCT server FROM resource_rule_state WHERE rule = ? AND firing", String::class.java, ResourceRule.SILENT.wireName)
                .filterNotNull().toSet()
        }
    } catch (error: DataAccessException) {
        if (generateSequence<Throwable>(error) { it.cause }.any { it is SQLException && it.sqlState == UNDEFINED_TABLE }) emptySet() else throw error
    }

    /** 한 트랜잭션 — 그 안의 조회는 [readTimeout] 에서 끊긴다(상한은 이 트랜잭션에만 — 커밋 · 되돌림과 함께 사라진다) */
    private fun <T : Any> limited(block: () -> T): T = transaction.execute {
        jdbc.queryForObject("SELECT set_config('statement_timeout', ?, true)", String::class.java, readTimeout.toMillis().toString())
        block()
    }!!

    private companion object {
        /** 권고 잠금의 번호 — 로그 무수신 판정만 쓴다(자원 판정 · 보내기의 잠금과 다르다) */
        const val LOG_JUDGE_LOCK = 7_310_420_003L
        /** 권고 잠금의 번호 — 로그 글자 규칙 판정만 쓴다(무수신 판정의 잠금과 달라 서로 기다리지 않는다) */
        const val LOG_PATTERN_LOCK = 7_310_420_004L
        const val UNDEFINED_TABLE = "42P01"   // PostgreSQL: relation does not exist

        fun ResultSet.nullableInstant(column: String): Instant? = getLong(column).takeUnless { wasNull() }?.let(Instant::ofEpochMilli)

        fun ResultSet.toPatternState() = LogPatternState(
            name = getString("name"), checkedUntil = Instant.ofEpochMilli(getLong("checked_until_ms")), lastNotifiedAt = nullableInstant("last_notified_ms"),
            pendingCount = getLong("pending_count"), pendingFirstRequestId = getString("pending_first_request_id"), pendingFirstAt = nullableInstant("pending_first_ms"),
        )

        fun ResultSet.toState() = LogRuleState(
            service = getString("service"),
            rule = getString("rule").let { wireName -> LogRule.entries.firstOrNull { it.wireName == wireName } ?: throw IllegalStateException("알 수 없는 로그 규칙: $wireName") },
            firing = getBoolean("firing"), since = nullableInstant("since_ms"), lastNotifiedAt = nullableInstant("last_notified_ms"),
            lastReceivedAt = nullableInstant("last_received_ms"),
        )
    }
}
