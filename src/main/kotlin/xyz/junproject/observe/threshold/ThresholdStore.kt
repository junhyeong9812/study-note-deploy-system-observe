package xyz.junproject.observe.threshold

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.transaction.support.TransactionTemplate
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant

/** 판정 한 바퀴의 결과 — 잠금을 얻지 못했으면(다른 자리가 판정 중) [judged] 는 false 이고 아무것도 바꾸지 않았다 */
data class RoundOutcome(val judged: Boolean, val eventsCreated: Int)

/** 보낼 표의 한 줄 — [attempts] 는 지금까지 보낸 횟수, [lastFailure] 는 마지막 실패 사유 */
data class QueuedAlert(val id: Long, val alert: ResourceAlert, val attempts: Int, val lastFailure: String?)

/** 보낼 표의 건수 — [undelivered] 는 아직 보내지 못한 것(다시 보낼 것), [rejected] 는 알림 서버가 거절해 다시 보내지 않는 것 */
data class AlertCounts(val undelivered: Long, val rejected: Long)

/** `GET /metrics/latest` 의 `firing[]` 한 줄 — `{server, rule, target, severity, since, last_value}` (target 은 재시작 규칙의 컨테이너 이름, 그 밖은 null) */
data class FiringRule(val server: String, val rule: String, val target: String?, val severity: String, val since: Instant, val lastValue: Double?)

/** 지금 넘어 있는 규칙(서버 · 규칙 · 대상 순) */
interface FiringSource {
    fun firing(): List<FiringRule>
}

/** 판정 상태 저장 — 한 바퀴가 DB 잠금 · 상태 · 보낼 알림을 **한 트랜잭션**에 */
interface RuleStateStore {
    /**
     * 판정 잠금(자리 둘 중 한쪽만 — PostgreSQL 잠금)을 **기다리지 않고** 시도해, 얻으면 같은 트랜잭션 안에서 앞 상태를 읽어 [judge] 에 넘기고
     * 돌려받은 상태 전부와 알림을 기록한 뒤 커밋한다. 얻지 못하면 [judge] 를 부르지 않고 `judged = false`.
     * 같은 [ResourceAlert.eventId] 의 알림은 한 번만 남는다. 도중의 어떤 실패도 상태와 알림을 함께 되돌리고 예외로 낸다.
     */
    fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome
}

/** 보낼 표 — 보내는 쪽([AlertDispatcher])이 쓰는 면 */
interface AlertQueue {
    /** 아직 보내지 못했고 거절되지 않은 알림 — 오래된 것부터 [limit] 건 */
    fun pendingAlerts(limit: Int): List<QueuedAlert>
    /** 알림 서버가 받았다 — 다시 보내지 않는다 */
    fun markDelivered(id: Long, at: Instant)
    /** 보내지 못했다 — 남겨 두고 다음 바퀴에 다시 보낸다(횟수 · 사유를 남긴다) */
    fun markRetry(id: Long, at: Instant, reason: String)
    /** 알림 서버가 거절했다(다시 보내도 같은 답) — 다시 보내지 않고 거절 건수로 센다 */
    fun markRejected(id: Long, at: Instant, reason: String)
    /** 보내기 잠금(자리 둘 중 한쪽만)을 기다리지 않고 시도해 얻으면 [block] 을 돌리고 그 값을, 얻지 못하면 null */
    fun <T> whileHoldingDispatchLock(block: () -> T): T?
    fun alertCounts(): AlertCounts

    /**
     * 리뷰 루프 2(§16 · §16.1 — 구현 전): 대상([AlertTarget])마다 아직 보내지 못했고 거절되지 않은 알림의 가장 오래된 [perTarget] 건(오래된 것부터)과
     * 그 대상을 마지막으로 시도한 시각(보낸 적 없으면 null). 대상 수에는 제한이 없다 — 한 대상에 알림이 아무리 많아도 다른 대상의 머리가 읽힌다.
     * 마지막 시도 시각은 [markDelivered] · [markRetry] · [markRejected] 의 `at` 이다(받은 · 거절된 알림이 지워져도 대상의 시각은 남는다).
     */
    fun pendingByTarget(perTarget: Int): List<TargetQueue> = TODO("not implemented")
}

/**
 * 보내기의 대상(§16) — 서버 · 규칙의 묶음 · 대상 이름. 디스크 경고와 위험은 한 묶음(`"disk"`), 그 밖은 규칙 글자 그대로.
 * 대상 이름은 재시작 규칙의 컨테이너 이름, 그 밖은 null.
 */
data class AlertTarget(val server: String, val ruleGroup: String, val targetName: String?) {
    companion object {
        /** 알림 하나의 대상(구현 전) */
        fun of(alert: ResourceAlert): AlertTarget = TODO("not implemented")
    }
}

/** 한 대상의 줄 — [alerts] 는 오래된 것부터, [lastAttemptAt] 은 그 대상을 마지막으로 시도한 시각(없으면 null) */
data class TargetQueue(val target: AlertTarget, val lastAttemptAt: Instant?, val alerts: List<QueuedAlert>)

/**
 * PostgreSQL 저장소 — 표는 추가만(로그 · 로그인 시도 · 지표 표는 건드리지 않는다). 표 이름과 꼴은 구현이 정한다.
 * [failpoint] 는 테스트의 이음매 — 한 바퀴의 트랜잭션 안에서 상태를 쓴 직후 `"states_written"`, 알림을 쓴 직후 `"events_written"` 으로
 * 부른다(예외를 던지면 그 바퀴가 통째로 되돌려져야 한다). 운영에서는 아무것도 하지 않는다.
 *
 * 표(시각은 epoch 밀리초):
 * - `resource_rule_state`: 판정 상태 한 줄 = (서버, 규칙, 대상). 한 바퀴가 판정 뒤의 상태 전부로 바꿔 쓴다.
 * - `resource_alert_outbox`: 보낼 알림 — `event_id` 가 고유(같은 판정이 두 번 기록돼도 한 건). 받은 것은 지우고, 거절된 것은 `rejected_ms` 를 남겨 센다.
 *
 * 잠금 — 둘 다 PostgreSQL 의 권고 잠금(`pg_try_advisory_xact_lock`, 기다리지 않는다)이라 표 잠금이 아니다.
 * 지표 저장 · 조회 · 5분 평균(그쪽 `LOCK TABLE metric_sample IN SHARE MODE`) · 로그와 서로 막지 않는다.
 * - 판정 잠금: [judgeRound] 의 트랜잭션 하나(앞 상태 읽기 → 판정 → 상태 · 알림 쓰기 → 커밋) 동안.
 * - 보내기 잠금: [whileHoldingDispatchLock] 동안 따로 잡은 연결 하나의 트랜잭션이 쥔다. 그 안의 읽기 · 표시는 다른 연결에서 바로 커밋된다 —
 *   보낸 뒤 죽어도 이미 표시한 알림은 다시 보내지 않는다. 자리가 죽으면 연결이 끊겨 잠금이 풀린다.
 */
class ThresholdStore(
    private val jdbc: JdbcTemplate,
    private val failpoint: (String) -> Unit = {},
) : RuleStateStore, AlertQueue, FiringSource {
    private val dataSource = requireNotNull(jdbc.dataSource) { "JdbcTemplate 에 DataSource 가 없다" }
    private val transaction = TransactionTemplate(DataSourceTransactionManager(dataSource))
    @Volatile private var schemaReady = false
    private val creatingSchema = Any()

    /** 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema() {
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS resource_rule_state (
                 server           text             NOT NULL,
                 rule             text             NOT NULL,
                 target           text             NOT NULL,
                 target_name      text,
                 firing           boolean          NOT NULL,
                 since_ms         bigint,
                 last_notified_ms bigint,
                 last_value       double precision,
                 PRIMARY KEY (server, rule, target))"""
        )
        jdbc.execute(
            """CREATE TABLE IF NOT EXISTS resource_alert_outbox (
                 id              bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                 event_id        text    NOT NULL UNIQUE,
                 alert_key       text    NOT NULL,
                 kind            text    NOT NULL,
                 severity        text    NOT NULL,
                 title           text    NOT NULL,
                 detail          text    NOT NULL,
                 server          text    NOT NULL,
                 rule            text    NOT NULL,
                 target_name     text,
                 created_ms      bigint  NOT NULL,
                 attempts        integer NOT NULL DEFAULT 0,
                 last_failure    text,
                 last_attempt_ms bigint,
                 rejected_ms     bigint)"""
        )
        jdbc.execute("CREATE INDEX IF NOT EXISTS resource_alert_outbox_pending ON resource_alert_outbox (created_ms, id) WHERE rejected_ms IS NULL")
        schemaReady = true
    }

    /** 처음 닿을 때 만든다 — 뜬 직후 DB 가 없었어도 다음 바퀴 · 조회가 만든다 */
    private fun ensureSchema() {
        if (!schemaReady) synchronized(creatingSchema) { if (!schemaReady) createSchema() }
    }

    /** 뜬 직후 한 번 미리 만든다 — 실패는 예외로(부르는 쪽이 로그만 남기고 삼킨다) */
    fun prepareSchema() = ensureSchema()

    /** 저장된 상태 전부(테스트 · 확인용) */
    fun states(): List<RuleState> {
        ensureSchema()
        return jdbc.query("SELECT * FROM resource_rule_state ORDER BY server, rule, target", { row, _ -> row.toRuleState() })
    }

    override fun judgeRound(now: Instant, judge: (previous: List<RuleState>) -> JudgeResult): RoundOutcome {
        ensureSchema()
        return transaction.execute {
            val locked = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean::class.java, JUDGE_LOCK)!!
            if (!locked) return@execute RoundOutcome(false, 0)
            val previous = jdbc.query("SELECT * FROM resource_rule_state ORDER BY server, rule, target", { row, _ -> row.toRuleState() })
            val result = judge(previous)
            jdbc.update("DELETE FROM resource_rule_state")
            if (result.states.isNotEmpty()) jdbc.batchUpdate(
                """INSERT INTO resource_rule_state (server, rule, target, target_name, firing, since_ms, last_notified_ms, last_value)
                   VALUES (?,?,?,?,?,?,?,?)""",
                result.states, result.states.size,
            ) { statement, state ->
                statement.setString(1, state.server); statement.setString(2, state.rule.wireName); statement.setString(3, state.target)
                statement.setNullable(4, state.targetName, Types.VARCHAR); statement.setBoolean(5, state.firing)
                statement.setNullable(6, state.since?.toEpochMilli(), Types.BIGINT); statement.setNullable(7, state.lastNotifiedAt?.toEpochMilli(), Types.BIGINT)
                statement.setNullable(8, state.lastValue, Types.DOUBLE)
            }
            failpoint("states_written")
            val inserted = if (result.events.isEmpty()) 0 else jdbc.batchUpdate(
                """INSERT INTO resource_alert_outbox (event_id, alert_key, kind, severity, title, detail, server, rule, target_name, created_ms)
                   VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (event_id) DO NOTHING""",
                result.events, result.events.size,
            ) { statement, alert ->
                statement.setString(1, alert.eventId); statement.setString(2, alert.key); statement.setString(3, alert.kind)
                statement.setString(4, alert.severity.wireName); statement.setString(5, alert.title); statement.setString(6, alert.detail)
                statement.setString(7, alert.server); statement.setString(8, alert.rule.wireName); statement.setNullable(9, alert.targetName, Types.VARCHAR)
                statement.setLong(10, alert.createdAt.toEpochMilli())
            }.flatMap { it.asList() }.count { it > 0 || it == java.sql.Statement.SUCCESS_NO_INFO }
            failpoint("events_written")
            RoundOutcome(true, inserted)
        }!!
    }

    override fun pendingAlerts(limit: Int): List<QueuedAlert> {
        ensureSchema()
        return jdbc.query(
            "SELECT * FROM resource_alert_outbox WHERE rejected_ms IS NULL ORDER BY created_ms, id LIMIT ?",
            { row, _ -> QueuedAlert(row.getLong("id"), row.toAlert(), row.getInt("attempts"), row.getString("last_failure")) },
            limit,
        )
    }

    override fun markDelivered(id: Long, at: Instant) {
        jdbc.update("DELETE FROM resource_alert_outbox WHERE id = ?", id)
    }

    override fun markRetry(id: Long, at: Instant, reason: String) {
        jdbc.update("UPDATE resource_alert_outbox SET attempts = attempts + 1, last_failure = ?, last_attempt_ms = ? WHERE id = ?",
            reason.take(MAX_REASON), at.toEpochMilli(), id)
    }

    override fun markRejected(id: Long, at: Instant, reason: String) {
        jdbc.update("UPDATE resource_alert_outbox SET attempts = attempts + 1, last_failure = ?, last_attempt_ms = ?, rejected_ms = ? WHERE id = ?",
            reason.take(MAX_REASON), at.toEpochMilli(), at.toEpochMilli(), id)
    }

    /**
     * 보내기 잠금 — 이 풀에서 연결 하나를 따로 잡아 그 트랜잭션에서 권고 잠금을 시도한다(기다리지 않는다). [block] 의 읽기 · 표시는
     * 그 연결이 아닌 다른 연결에서 자동 커밋으로 돈다(이 풀은 연결이 둘 이상이어야 한다). 끝나면 그 트랜잭션을 되돌려 잠금을 푼다.
     */
    override fun <T> whileHoldingDispatchLock(block: () -> T): T? {
        ensureSchema()
        val connection = dataSource.connection
        try {
            connection.autoCommit = false
            val locked = connection.prepareStatement("SELECT pg_try_advisory_xact_lock(?)").use { statement ->
                statement.setLong(1, DISPATCH_LOCK)
                statement.executeQuery().use { it.next() && it.getBoolean(1) }
            }
            return if (locked) block() else null
        } finally {
            try { connection.rollback() } finally {
                try { connection.autoCommit = true } finally { DataSourceUtils.releaseConnection(connection, dataSource) }
            }
        }
    }

    override fun alertCounts(): AlertCounts {
        ensureSchema()
        return jdbc.queryForObject(
            "SELECT count(*) FILTER (WHERE rejected_ms IS NULL) AS undelivered, count(*) FILTER (WHERE rejected_ms IS NOT NULL) AS rejected FROM resource_alert_outbox",
            { row, _ -> AlertCounts(row.getLong("undelivered"), row.getLong("rejected")) },
        )!!
    }

    override fun firing(): List<FiringRule> {
        ensureSchema()
        return jdbc.query("SELECT * FROM resource_rule_state WHERE firing", { row, _ -> row.toRuleState() })
            .map { state ->
                FiringRule(state.server, state.rule.wireName, state.targetName.takeIf { state.rule == ResourceRule.RESTARTS },
                    (if (state.rule == ResourceRule.DISK_CRITICAL) AlertSeverity.CRITICAL else AlertSeverity.WARNING).wireName,
                    state.since ?: Instant.EPOCH, state.lastValue)
            }
            .sortedWith(compareBy({ it.server }, { it.rule }, { it.target ?: "" }))
    }

    private companion object {
        /** 권고 잠금의 번호 — 이 저장소만 쓴다(판정 · 보내기를 따로) */
        const val JUDGE_LOCK = 7_310_420_001L
        const val DISPATCH_LOCK = 7_310_420_002L
        const val MAX_REASON = 200

        fun PreparedStatement.setNullable(index: Int, value: Any?, sqlType: Int) =
            if (value == null) setNull(index, sqlType) else setObject(index, value, sqlType)

        fun ResultSet.nullableInstant(column: String): Instant? = getLong(column).takeUnless { wasNull() }?.let(Instant::ofEpochMilli)

        fun rule(wireName: String) = ResourceRule.entries.firstOrNull { it.wireName == wireName } ?: throw IllegalStateException("알 수 없는 규칙: $wireName")

        fun ResultSet.toRuleState() = RuleState(
            server = getString("server"), rule = rule(getString("rule")), target = getString("target"), targetName = getString("target_name"),
            firing = getBoolean("firing"), since = nullableInstant("since_ms"), lastNotifiedAt = nullableInstant("last_notified_ms"),
            lastValue = getDouble("last_value").takeUnless { wasNull() },
        )

        fun ResultSet.toAlert() = ResourceAlert(
            eventId = getString("event_id"), key = getString("alert_key"), kind = getString("kind"),
            severity = AlertSeverity.entries.firstOrNull { it.wireName == getString("severity") } ?: throw IllegalStateException("알 수 없는 severity"),
            title = getString("title"), detail = getString("detail"), server = getString("server"), rule = rule(getString("rule")),
            targetName = getString("target_name"), createdAt = Instant.ofEpochMilli(getLong("created_ms")),
        )
    }
}
