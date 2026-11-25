package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.traffic.TrafficCellQuery
import xyz.junproject.observe.traffic.TrafficClass
import xyz.junproject.observe.traffic.TrafficController
import xyz.junproject.observe.traffic.TrafficDistribution
import xyz.junproject.observe.traffic.TrafficReadProgress
import xyz.junproject.observe.traffic.TrafficRecords
import xyz.junproject.observe.traffic.TrafficStore
import xyz.junproject.observe.traffic.TrafficTotals
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import javax.sql.DataSource

/**
 * 실제 PostgreSQL — 지도(`GET /traffic/map`)의 합을 DB 에서 낸다(리뷰 루프 1 의 4번): 응답의 꼴과 값은 칸을 하나씩 읽어 합친 것과 같고,
 * "채우는 중"은 칸마다 **그 칸의 서버의** 읽기 진도로 갈리며(진도가 없는 서버의 칸은 모두 채우는 중), 저장소의 지도용 읽기
 * `TrafficStore.edgeTotals(from, to)` 가 돌려주는 행의 수는 칸이 아니라 선 × 서버 × 2(채우는 중 · 채워진) 이하다. 표는 전용 스키마(`traffic_map`).
 * 원본 칸을 많이 만드는 사례는 칸 표(`traffic_segment` — 계약의 칸 이름)에 SQL 로 직접 넣는다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TrafficMapEngineTest {
    private val schema = "traffic_map"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@TrafficMapEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = TrafficStore(jdbc, jdbc, jdbc)
    private val controller = TrafficController(store, "map-secret")
    private val now = Instant.parse("2026-11-25T04:00:00Z")

    /** DB 에서 실제로 받아 온 행의 수를 세는 연결 — 조회 연결로 쓴다(ResultSet.next() 가 참을 낸 수) */
    private val fetchedRows = AtomicLong()
    private fun <T> counting(target: T, type: Class<T>): T = type.cast(Proxy.newProxyInstance(type.classLoader, arrayOf(type), InvocationHandler { _, method, arguments ->
        val result = try { method.invoke(target, *(arguments ?: emptyArray())) } catch (error: InvocationTargetException) { throw error.targetException }
        when {
            result is ResultSet && method.returnType == ResultSet::class.java -> counting(result, ResultSet::class.java)
            result is Statement && Statement::class.java.isAssignableFrom(method.returnType) -> counting(result, method.returnType as Class<Any>)
            method.declaringClass == ResultSet::class.java && method.name == "next" && result == true -> { fetchedRows.incrementAndGet(); result }
            else -> result
        }
    }))
    private val countingQuery = JdbcTemplate(counting(dataSource as DataSource, DataSource::class.java).let { source ->
        object : DataSource by source {
            override fun getConnection(): Connection = counting(source.connection, Connection::class.java)
        }
    })
    private val countedController = TrafficController(TrafficStore(jdbc, countingQuery, jdbc), "map-secret")

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    @Suppress("UNCHECKED_CAST")
    private fun edges(from: Instant, to: Instant): List<Map<String, Any?>> {
        val response: ResponseEntity<out Any> = controller.map(MockHttpServletRequest("GET", "/traffic/map").apply { addHeader("X-Observe-Secret", "map-secret") },
            from.toString(), to.toString(), now)
        assertEquals(200, response.statusCode.value(), "${response.body}")
        return ((response.body as Map<String, Any?>)["data"] as Map<String, Any?>)["edges"] as List<Map<String, Any?>>
    }

    /** 진도를 만드는 항목 하나 — [at] 이 그 서버의 읽기 진도가 된다 */
    private fun progress(server: String, at: Instant, segments: List<String> = emptyList()) {
        store.store(TrafficRecords.entry(1, at, now, TrafficRecords.body(segments), server = server))
    }

    /** 원본 칸을 SQL 로 바로 넣는다 — 진도가 없는 서버의 칸 · 많은 칸 */
    private fun insertCells(sql: String, vararg arguments: Any) = administrationJdbc.update(sql.replace("{schema}", schema), *arguments)

    /**
     * 지도용 읽기 — 이름과 꼴이 계약이다: `TrafficStore.edgeTotals(from: Instant, to: Instant): List<TrafficEdgeTotals>`,
     * `TrafficEdgeTotals(server, service, caller, trafficClass, filling, totals)`. 아직 없는 구현에서도 이 파일이 컴파일되게 이름으로 부른다.
     */
    private fun edgeTotals(from: Instant, to: Instant): List<List<Any?>> {
        val method = store.javaClass.getMethod("edgeTotals", Instant::class.java, Instant::class.java)
        return (method.invoke(store, from, to) as List<*>).map { row ->
            listOf("server", "service", "caller", "trafficClass", "filling", "totals").map { name ->
                row!!.javaClass.getMethod("get" + name.replaceFirstChar { it.uppercase() }).invoke(row)
            }
        }
    }

    @Test
    fun `같은 선이 두 서버에 걸치면 채우는 중의 경계는 칸마다 그 칸의 서버의 읽기 진도로 갈린다`() {
        // 진도: ci-agent-158 = now(경계 now − 30초) · ci-agent-164 = now − 2분(경계 now − 2분 30초)
        val cells = listOf(now.minusSeconds(90), now.minusSeconds(60), now.minusSeconds(30))           // 끝: now − 60초 · now − 30초 · now
        progress("ci-agent-158", now, cells.map { TrafficRecords.segment(it, listOf(200, 500)) })
        progress("ci-agent-164", now.minus(Duration.ofMinutes(2)), cells.map { TrafficRecords.segment(it, listOf(200, 200, 200)) })
        val edge = edges(now.minus(Duration.ofMinutes(5)), now).single()
        assertEquals(4L, edge["count"], "채워진 칸은 158 의 앞 두 칸(끝이 경계와 같은 칸 포함)뿐이다")
        assertEquals(0.5, edge["error_rate"])
        assertEquals(2L + 9L, edge["filling_count"], "158 의 마지막 칸과 164 의 세 칸이 채우는 중이다")
    }

    @Test
    fun `진도가 없는 서버의 칸은 오래됐어도 모두 채우는 중이고 수치에 들지 않는다`() {
        progress("ci-agent-158", now, listOf(TrafficRecords.segment(now.minus(Duration.ofMinutes(10)), listOf(200))))
        insertCells("""INSERT INTO {schema}.traffic_segment (server, bucket_ms, service, caller, class, count, status_2xx, status_3xx, status_4xx, status_5xx,
                         status_other, buckets, max_ms, generation, seq, received_ms)
                       VALUES ('ghost', ?, 'backend', 'front', 'request', 5, 0, 0, 0, 5, 0, ARRAY[5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[], 4, 'aaaaaaaaaaaaaaaa', 1, ?)""",
            now.minus(Duration.ofMinutes(20)).toEpochMilli(), now.toEpochMilli())
        val edge = edges(now.minus(Duration.ofMinutes(30)), now).single()
        assertEquals(1L, edge["count"])
        assertEquals(0.0, edge["error_rate"], "진도가 없는 서버의 5xx 가 판정 수치에 들었다")
        assertEquals(5L, edge["filling_count"])
    }

    @Test
    fun `지도용 읽기가 돌려주는 행은 칸이 아니라 선 × 서버 × 2 이하이고 그 합이 칸의 합과 같다`() {
        for (server in listOf("ci-agent-158", "ci-agent-164")) progress(server, now.minus(Duration.ofMinutes(30)))
        insertCells("""INSERT INTO {schema}.traffic_segment (server, bucket_ms, service, caller, class, count, status_2xx, status_3xx, status_4xx, status_5xx,
                         status_other, buckets, max_ms, generation, seq, received_ms)
                       SELECT server_name, ? + cell * 30000, 'backend', 'caller-' || line, 'request', 2, 2, 0, 0, 0, 0,
                              ARRAY[2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[], 3, 'aaaaaaaaaaaaaaaa', 1, ?
                       FROM unnest(ARRAY['ci-agent-158', 'ci-agent-164', 'ghost']) server_name, generate_series(0, 3) line, generate_series(0, 119) cell""",
            now.minus(Duration.ofHours(1)).toEpochMilli(), now.toEpochMilli())
        val rows = edgeTotals(now.minus(Duration.ofHours(1)), now)
        assertTrue(rows.size <= 4 * 3 * 2, "행이 칸에 비례한다: ${rows.size}")
        assertEquals(rows.size, rows.map { it.take(5) }.toSet().size, "같은 (서버, 선, 채우는 중) 이 두 행으로 나뉘었다")
        assertEquals(3L * 4 * 120 * 2, rows.sumOf { (it[5] as TrafficTotals).count })
        assertTrue(rows.filter { it[0] == "ghost" }.all { it[4] == true }, "진도가 없는 서버의 칸이 채워진 것으로 묶였다")
    }

    @Test
    fun `칸 수만 개에서도 지도의 값은 칸을 하나씩 읽어 메모리에서 합친 것과 같다`() {
        val from = now.minus(Duration.ofHours(24))
        for (server in 0..2) progress("big-$server", now.minus(Duration.ofMinutes(7L * server)))         // big-3 은 진도가 없다
        val cellsSql = """INSERT INTO {schema}.traffic_segment (server, bucket_ms, service, caller, class, count, status_2xx, status_3xx, status_4xx, status_5xx,
                            status_other, buckets, max_ms, generation, seq, received_ms)
                          SELECT 'big-' || server_index, ? + cell * 30000, 'backend', 'caller-' || line, 'request', amount, amount - amount % 3, 0, 0, amount % 3, 0,
                                 (SELECT array_agg(CASE WHEN position = (cell + line) % 20 THEN amount ELSE 0 END ORDER BY position) FROM generate_series(0, 19) position),
                                 1 + (cell * 37 + line * 11 + server_index) % 5000, 'aaaaaaaaaaaaaaaa', 1, ?
                          FROM generate_series(0, 3) server_index, generate_series(0, 4) line, generate_series(0, 2879) cell,
                               LATERAL (SELECT 1 + (cell * 7 + line * 3 + server_index + ?) % 50 AS amount) amount_row
                          WHERE cell % ? = 0"""
        assertEquals(4 * 5 * 2880, insertCells(cellsSql, from.toEpochMilli(), now.toEpochMilli(), 0, 1))
        insertCells(cellsSql, from.toEpochMilli(), now.toEpochMilli(), 13, 4)                            // 넷 중 한 칸은 원본 행이 둘
        // 참조 — 칸을 하나씩 읽어 서버마다의 진도로 가르고 메모리에서 합친다(바꾸기 전의 지도 계산 그대로)
        val progress = store.serverProgress().associate { it.server to it.readProgressAt }
        val expected = store.cells(TrafficCellQuery(from, now, from)).groupBy { Triple(it.service, it.caller, it.trafficClass) }
            .toSortedMap(compareBy({ it.first }, { it.second }, { it.third.wireName }))
            .map { (key, cells) ->
                val (filling, settled) = cells.partition { TrafficReadProgress.isFilling(it.bucketStart.plus(it.width), progress[it.server]) }
                val totals = settled.fold(TrafficTotals.EMPTY) { sum, cell -> sum + cell.totals }
                fun quantile(percent: Int) = TrafficDistribution.quantile(totals.buckets, percent)?.let { if (it.atMostMs != null) mapOf("le_ms" to it.atMostMs) else mapOf("gt_ms" to it.overMs) }
                mapOf("service" to key.first, "caller" to key.second, "class" to key.third.wireName, "count" to totals.count,
                    "status" to mapOf("2xx" to totals.status.status2xx, "3xx" to totals.status.status3xx, "4xx" to totals.status.status4xx,
                        "5xx" to totals.status.status5xx, "other" to totals.status.other),
                    "error_rate" to (if (totals.count == 0L) null else totals.status.status5xx.toDouble() / totals.count),
                    "p50" to quantile(50), "p95" to quantile(95), "max_ms" to totals.maxMs, "filling_count" to filling.sumOf { it.totals.count })
            }
        assertEquals(5, expected.size)
        assertTrue(expected.all { (it["filling_count"] as Long) > 0 && (it["count"] as Long) > 0 }, "참조가 채우는 중 · 채워진 칸을 모두 담지 않았다")
        assertEquals(expected, edges(from, now))
        // DB 에서 받아 오는 행은 선 · 서버 · 채우는 중마다 — 칸 수(약 7만 2천)가 아니다. 진도 · 요약을 읽는 행까지 넉넉히 더해 둔다
        fetchedRows.set(0)
        val response = countedController.map(MockHttpServletRequest("GET", "/traffic/map").apply { addHeader("X-Observe-Secret", "map-secret") },
            from.toString(), now.toString(), now)
        assertEquals(200, response.statusCode.value())
        assertTrue(fetchedRows.get() <= 5 * 4 * 2 + 100, "지도 한 번에 DB 에서 ${fetchedRows.get()} 행을 받아 왔다 — 칸을 하나씩 읽는다")
        assertEquals(TrafficClass.REQUEST.wireName, expected.first()["class"])
    }
}
