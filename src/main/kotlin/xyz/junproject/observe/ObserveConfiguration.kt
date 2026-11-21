package xyz.junproject.observe

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.RedisLogStreamSource
import xyz.junproject.observe.log.LogStore

/** 조립 — 연결 정보는 환경변수에서. DB 비밀번호 같은 값은 로그에 내지 않는다. */
@Configuration
class ObserveConfiguration {
    private val log = LoggerFactory.getLogger("observe")

    private fun pool(name: String, url: String, user: String, password: String, size: Int, statementTimeoutMs: Int, extraInitSql: String = "") =
        HikariDataSource(HikariConfig().apply {
            poolName = name
            jdbcUrl = url
            username = user
            this.password = password
            maximumPoolSize = size
            connectionTimeout = 5_000
            // 뜰 때 DB 가 아직 준비 전이어도 죽지 않는다 — 수집 바퀴가 실패로 남기고 다시 시도한다
            initializationFailTimeout = -1
            connectionInitSql = "SET statement_timeout = $statementTimeoutMs$extraInitSql"
        })

    /** 수집과 조회의 연결 풀을 나눈다 — 무거운 조회가 연결을 다 잡아도 수집은 밀리지 않는다. 조회는 10초에서 끊는다. */
    @Bean
    fun logStore(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
    ): LogStore {
        val ingest = pool("observe-ingest", url, user, password, size = 2, statementTimeoutMs = 30_000)
        // 조회는 늘 값을 보고 계획한다 — 검색어(q)가 자리표시자라 같은 문장이 되풀이되면 PostgreSQL 이 값과 무관한 일반 계획으로
        // 바꿀 수 있는데, 그 계획으로는 흔한 말 검색이 75만 행에서 2초 걸렸다(값을 보고 세운 계획은 1ms). 계획 비용은 1ms 안팎.
        val query = pool("observe-query", url, user, password, size = 4, statementTimeoutMs = 10_000,
            extraInitSql = "; SET plan_cache_mode = force_custom_plan")
        return LogStore(JdbcTemplate(ingest), JdbcTemplate(query))
    }

    @Bean
    fun logStreamSource(
        redis: StringRedisTemplate,
        @Value("\${observe.stream:logs}") stream: String,
        @Value("\${observe.group:observe}") group: String,
    ): RedisLogStreamSource {
        log.info("observe:observe:collecting stream '{}' as group '{}'", stream, group)
        return RedisLogStreamSource(redis, stream, group)
    }

    @Bean
    fun logCollector(source: RedisLogStreamSource, store: LogStore) = LogCollector(source, store)

    @Bean
    fun collectorHealth(collector: LogCollector, source: RedisLogStreamSource, store: LogStore) =
        CollectorHealth(collector, { source.unacknowledgedCount() }, { store.ping() })
}
