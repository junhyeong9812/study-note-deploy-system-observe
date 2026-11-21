package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.LogStore
import java.net.URI
import java.time.Duration

/**
 * 실제 엔진 테스트의 접속 — scripts/engine-test.sh 가 환경변수로 넘긴다.
 * 값이 없으면 건너뛰지 않고 실패한다(건너뛴 테스트는 통과로 보이기 때문).
 */
object EngineSupport {
    private fun environment(name: String) = requireNotNull(System.getenv(name)?.takeIf { it.isNotBlank() }) {
        "$name 이 없다 — scripts/engine-test.sh 로 돌린다"
    }

    val databaseUrl get() = environment("OBSERVE_TEST_DB_URL")              // jdbc:postgresql://host:5432/observe
    val databaseUser get() = environment("OBSERVE_TEST_DB_USER")
    val databasePassword get() = environment("OBSERVE_TEST_DB_PASSWORD")
    private val redisUrl get() = environment("OBSERVE_TEST_REDIS_URL")        // redis://host:6379

    fun dataSource(size: Int = 4) = HikariDataSource(HikariConfig().apply {
        jdbcUrl = databaseUrl; username = databaseUser; password = databasePassword; maximumPoolSize = size
    })

    /** 수집·조회 풀을 나누지 않은 단순 저장소 — 저장소 동작만 볼 때 */
    fun store(dataSource: HikariDataSource): LogStore = LogStore(JdbcTemplate(dataSource), JdbcTemplate(dataSource))

    fun resetTable(dataSource: HikariDataSource) {
        store(dataSource).createSchema()
        JdbcTemplate(dataSource).execute("TRUNCATE log_entry")
    }

    /** 프로세스 하나에 해당하는 Redis 연결 — 수집기 둘을 서로 다른 연결로 돌려 두 인스턴스를 흉내 낸다 */
    fun redisConnection(): LettuceConnectionFactory {
        val uri = URI(redisUrl)
        val factory = LettuceConnectionFactory(
            RedisStandaloneConfiguration(uri.host, if (uri.port > 0) uri.port else 6379),
            LettuceClientConfiguration.builder().commandTimeout(Duration.ofSeconds(10)).build(),   // application.yml 과 같은 10초
        )
        factory.afterPropertiesSet()
        factory.start()
        return factory
    }

    fun redisTemplate(factory: LettuceConnectionFactory) = StringRedisTemplate(factory).also { it.afterPropertiesSet() }
}
