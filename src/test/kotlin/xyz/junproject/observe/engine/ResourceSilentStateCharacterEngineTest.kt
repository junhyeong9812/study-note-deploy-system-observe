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
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.AlertSender
import xyz.junproject.observe.threshold.ResourceAlert
import xyz.junproject.observe.threshold.SendOutcome
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import java.time.Instant

/**
 * 실제 PostgreSQL — 자원 끊김이 판정 상태 표(`resource_rule_state`)에 남는 꼴을 고정한다(로그 무수신 판정이 "그 서버가 끊김으로 넘어 있는가"를 이 표에서 읽는다):
 * 끊김이 넘어 있는 서버는 (서버, 규칙 `silent`, 대상 빈 글자, firing true) 한 줄 · 다시 받으면 firing true 인 끊김 줄이 없다(판정이 상태를 바꿔 쓴다). 표는 전용 스키마(`silent_state_character`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResourceSilentStateCharacterEngineTest {
    private val schema = "silent_state_character"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@ResourceSilentStateCharacterEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = ThresholdStore(jdbc)
    private val metricStore = MetricStore(jdbc, jdbc, jdbc)
    private val now = Instant.parse("2026-11-26T03:00:00Z")

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
        metricStore.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    private val sender = object : AlertSender { override fun send(alert: ResourceAlert) = SendOutcome.Delivered }

    private fun silentRows() = jdbc.queryForList("SELECT server, rule, target, firing FROM resource_rule_state WHERE rule = 'silent' ORDER BY server")

    @Test
    fun `자원 끊김이 넘어 있는 서버는 상태 표에 rule silent · 대상 빈 글자 · firing true 한 줄이고 다시 받으면 firing true 인 끊김 줄이 없다`() {
        val round = ThresholdRound(metricStore, store, ThresholdJudge(), AlertDispatcher(store, sender), store, setOf("ci-agent-164"))
        assertTrue(round.judgeOnce(now).judged)
        assertEquals(listOf(mapOf("server" to "ci-agent-164", "rule" to "silent", "target" to "", "firing" to true)), silentRows())

        val receivedAt = now.plusSeconds(30)
        metricStore.insertAll((0 until 10).map {
            val measuredAt = receivedAt.minusSeconds(30L * it)
            ResourceSample(StreamId(measuredAt.toEpochMilli(), it), "ci-agent-164", measuredAt, receivedAt, 30_000,
                HostValues(cpuPct = 5.0, load1 = 0.1, memTotal = 16_000, memUsed = 800, diskTotal = 1_000, diskUsed = 100), emptyList(), emptyList(), emptyList())
        })
        assertTrue(round.judgeOnce(receivedAt.plusSeconds(1)).judged)
        assertEquals(emptyList<Map<String, Any?>>(), silentRows().filter { it["firing"] == true }, "다시 받았는데 끊김이 넘어 있는 줄로 남았다")
    }
}
