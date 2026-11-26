package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import xyz.junproject.observe.ObserveConfiguration
import java.util.UUID

/**
 * 실제 앱의 연결 풀 — 이름과 크기를 고정한다(로그 무수신 판정은 판정 · 알림 풀을 함께 쓴다 — 자리당 연결 수가 바뀌지 않는다).
 * 표는 전용 스키마(`pools_character`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
class ObservePoolsCharacterEngineTest {
    @Autowired private lateinit var configuration: ObserveConfiguration

    @Test
    fun `앱이 만드는 연결 풀은 지금의 이름 아홉과 크기 그대로다 — 합 21`() {
        val field = ObserveConfiguration::class.java.getDeclaredField("pools").apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        val pools = (field.get(configuration) as List<HikariDataSource>).associate { it.poolName to it.maximumPoolSize }
        assertEquals(mapOf(
            "observe-ingest" to 2, "observe-query" to 4, "observe-health" to 1, "observe-auth" to 3,
            "observe-metric-ingest" to 2, "observe-metric-health" to 1, "observe-metric-health-detail" to 1,
            "observe-metric-query" to 2, "observe-threshold" to 5,
        ), pools, "연결 풀의 이름 · 크기가 바뀌었다(자리당 연결 수)")
        assertEquals(21, pools.values.sum())
    }

    @AfterAll fun cleanUp() = administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")

    companion object {
        private const val SCHEMA = "pools_character"
        private val suffix = UUID.randomUUID().toString().take(8)
        private val administration by lazy { JdbcTemplate(EngineSupport.dataSource(size = 1)) }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            administration.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public")
            administration.execute("DROP SCHEMA IF EXISTS $SCHEMA CASCADE")
            administration.execute("CREATE SCHEMA $SCHEMA")
            registry.add("observe.db.url") { EngineSupport.databaseUrl + "?currentSchema=$SCHEMA,public" }
            registry.add("observe.db.user") { EngineSupport.databaseUser }
            registry.add("observe.db.password") { EngineSupport.databasePassword }
            registry.add("spring.data.redis.url") { requireNotNull(System.getenv("OBSERVE_TEST_REDIS_URL")) { "OBSERVE_TEST_REDIS_URL 이 없다" } }
            registry.add("observe.stream") { "pools-character-logs-$suffix" }
            registry.add("observe.group") { "pools-character-$suffix" }
            registry.add("observe.metrics.stream") { "pools-character-metrics-$suffix" }
            registry.add("observe.metrics.group") { "pools-character-$suffix" }
        }
    }
}
