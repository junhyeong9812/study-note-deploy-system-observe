package xyz.junproject.observe.logalert

import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.threshold.RoundOutcome
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

/** PostgreSQL 저장소(`log_rule_state`) — 판정 · 알림 풀을 쓴다. [readTimeout] 은 판정의 조회의 시간 상한. 뼈대(03): 아직 없다 */
class LogAlertStore(private val jdbc: JdbcTemplate, private val readTimeout: Duration = Duration.ofSeconds(2)) : LogRuleStateStore, LogReceipts {
    fun createSchema(): Unit = TODO("03: 표 만들기")
    fun prepareSchema(): Unit = TODO("03: 표 만들기")
    fun states(): List<LogRuleState> = TODO("03: 상태 읽기")
    override fun judgeRound(now: Instant, judge: (previous: List<LogRuleState>) -> LogJudgeResult): RoundOutcome = TODO("03: 판정 한 바퀴")
    override fun lastReceived(services: Collection<String>): Map<String, Instant?> = TODO("03: 서비스마다의 마지막 로그")
    override fun lastReceivedAny(): Instant? = TODO("03: 전체의 마지막 로그")
    override fun silentServers(): Set<String> = TODO("03: 자원 끊김이 넘어 있는 서버")
}
