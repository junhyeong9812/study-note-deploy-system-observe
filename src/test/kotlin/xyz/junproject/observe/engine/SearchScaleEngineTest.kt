package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import xyz.junproject.observe.ObserveConfiguration
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogQuery
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * 30일치 규모(75만 행)에서 내용 부분 일치 검색 — load-bearing 가정 2: trigram 인덱스로 1초 안에 끝나는가.
 * 그리고 무거운 조회가 수집(저장)을 밀지 않는가 — 수집과 조회의 연결 풀이 나뉘어 있는가.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SearchScaleEngineTest {
    private val dataSource: HikariDataSource = EngineSupport.dataSource()
    private val jdbc = JdbcTemplate(dataSource)
    private val now = Instant.parse("2026-11-21T12:00:00Z")
    private val monthAgo = now.minus(Duration.ofDays(30))

    /** 저장소가 실제로 보낸 SQL 과 인자를 잡아 같은 것을 EXPLAIN 한다 — 테스트 안에 SQL 을 베끼지 않으려고 */
    private class CapturingJdbcTemplate(dataSource: HikariDataSource) : JdbcTemplate(dataSource) {
        var lastSql: String? = null
        var lastArguments: Array<out Any?> = emptyArray()
        override fun <T> query(sql: String, rowMapper: RowMapper<T>, vararg args: Any?): List<T> {
            lastSql = sql; lastArguments = args
            return super.query(sql, rowMapper, *args)
        }
    }
    private val capturing = CapturingJdbcTemplate(dataSource)
    private val explainingStore = LogStore(JdbcTemplate(dataSource), capturing)
    /** 운영과 같은 조립(풀 둘, statement_timeout) */
    private val configuredStore = ObserveConfiguration().logStore(EngineSupport.databaseUrl, EngineSupport.databaseUser, EngineSupport.databasePassword)

    @BeforeAll
    fun fill() {
        EngineSupport.resetTable(dataSource)        // 인덱스가 있는 채로 넣는다 — 운영과 같은 순서
        val started = System.nanoTime()
        // 75만 행을 30일에 고르게(3.456초 간격). 분포는 실측(front 57%·backend 41%·나머지 셋, error 0.3%·warning 0.6%)을 따른다.
        // 'quota-exceeded' = 결과가 많은 드문 말(250 행에 1 행 = 3000 행), 'kafka-rebalance' = 가장 오래된 하루에만 12 행,
        // 'status' = front 줄 전부에 있는 흔한 말.
        jdbc.execute(
            """INSERT INTO log_entry (stream_ms, stream_seq, level, service, request_id, message)
               SELECT ${monthAgo.toEpochMilli()} + i::bigint * 3456, 0,
                      CASE WHEN i % 1000 < 3 THEN 'error' WHEN i % 1000 < 9 THEN 'warning' ELSE 'info' END,
                      CASE WHEN i % 100 < 57 THEN 'front' WHEN i % 100 < 98 THEN 'backend' WHEN i % 100 = 98 THEN 'llm-wrapper' ELSE 'ci-agent' END,
                      'req-' || substr(md5((i / 5)::text), 1, 12),
                      CASE WHEN i % 250 = 7 THEN 'upstream quota-exceeded for model ' || (i % 13)
                           WHEN i IN (11, 2011, 4011, 6011, 8011, 10011, 12011, 14011, 16011, 18011, 20011, 22011) THEN 'kafka-rebalance stalled partition ' || i
                           WHEN i % 100 < 57 THEN 'GET /docs/' || (i % 9000) || ' status 200 ' || (i % 700) || 'ms etag=' || substr(md5(i::text), 1, 16)
                           ELSE 'document ' || (i % 20000) || ' rendered chunks=' || (i % 37) || ' trace=' || substr(md5(i::text), 1, 16) END
               FROM generate_series(0, 749999) AS i"""
        )
        jdbc.execute("ANALYZE log_entry")
        println("observe-test fill 750000 rows: ${Duration.ofNanos(System.nanoTime() - started).toMillis()} ms")
        assertEquals(750_000L, jdbc.queryForObject("SELECT count(*) FROM log_entry", Long::class.java))
    }

    @AfterAll fun close() { jdbc.execute("TRUNCATE log_entry"); dataSource.close() }

    private data class Measured(val results: Int, val milliseconds: List<Long>, val plan: String)

    private fun measure(text: String, limit: Int = 100): Measured {
        val query = LogQuery(monthAgo, now, text = text, limit = limit)
        // 12 번 — PgJDBC 는 같은 문장을 다섯 번 넘게 쓰면 서버 쪽 준비된 문장으로 바꾸고, PostgreSQL 은 그 뒤 값과 무관한
        // 일반 계획을 고를 수 있다. 처음 몇 번만 재면 운영에서 늘 쓰일 계획을 놓친다.
        val times = (1..12).map {
            val started = System.nanoTime()
            configuredStore.search(query)
            Duration.ofNanos(System.nanoTime() - started).toMillis()
        }
        val results = explainingStore.search(query).size
        val plan = jdbc.queryForList("EXPLAIN (ANALYZE, BUFFERS) ${capturing.lastSql}", String::class.java, *capturing.lastArguments).joinToString("\n")
        println("observe-test search q='$text' limit=$limit results=$results times(ms)=$times\n$plan\n")
        return Measured(results, times, plan)
    }

    @Test
    fun `결과가 많은 드문 말은 표 전체를 훑지 않고 1초 안에 끝난다`() {
        // 결과가 많으면 기본 키를 최신부터 거꾸로 읽다 limit 에서 멈추는 쪽이 trigram 보다 싸다 — 어느 인덱스든 표 전체를 훑지 않으면 된다
        for (limit in listOf(100, 500)) {
            val measured = measure("quota-exceeded", limit)
            assertEquals(limit, measured.results)
            assertFalse("Seq Scan on log_entry" in measured.plan, measured.plan)
            assertTrue(measured.milliseconds.max() < 1_000, "${measured.milliseconds}")
        }
    }

    @Test
    fun `가장 오래된 하루에만 있는 아주 드문 말도 1초 안에 끝난다`() {
        val measured = measure("kafka-rebalance")
        assertEquals(12, measured.results)
        assertTrue("log_entry_message_trgm" in measured.plan, measured.plan)
        assertTrue(measured.milliseconds.max() < 1_000, "${measured.milliseconds}")
    }

    @Test
    fun `흔한 말은 표 전체를 훑지 않고 1초 안에 끝난다`() {
        val measured = measure("status")
        assertEquals(100, measured.results)
        assertFalse("Seq Scan on log_entry" in measured.plan, measured.plan)
        assertTrue(measured.milliseconds.max() < 1_000, "${measured.milliseconds}")
    }

    @Test
    fun `무거운 검색어를 되풀이한 연결에서도 흔한 말이 1초 안에 끝난다`() {
        // 같은 문장을 무거운 값으로 여러 번 쓰면 PostgreSQL 이 일반 계획으로 바꿀 수 있다 — 그 뒤의 가벼운 검색이 느려지지 않는가
        val heavy = LogQuery(monthAgo, now, text = "zq")
        repeat(4) { (1..4).map { thread { repeat(3) { configuredStore.search(heavy) } } }.forEach { it.join() } }
        val times = (1..12).map {
            val started = System.nanoTime()
            configuredStore.search(LogQuery(monthAgo, now, text = "status"))
            Duration.ofNanos(System.nanoTime() - started).toMillis()
        }
        println("observe-test common word after heavy repeats times(ms)=$times")
        assertTrue(times.max() < 1_000, "$times")
    }

    @Test
    fun `두 글자 검색어의 시간을 잰다`() {
        // 명세는 q 를 2자 이상으로 열어 두었다 — trigram 은 세 글자 단위라 두 글자는 인덱스로 좁히지 못한다. 기록용으로 잰다.
        for (text in listOf("ms", "zq")) {
            val measured = measure(text)
            assertTrue(measured.milliseconds.max() < 10_000, "조회 statement_timeout(10초) 안에 끝나야 한다: ${measured.milliseconds}")
        }
    }

    @Test
    fun `무거운 조회가 몰려도 저장은 밀리지 않는다`() {
        val running = AtomicBoolean(true)
        val searches = AtomicInteger()
        val heavy = LogQuery(monthAgo, now, text = "zq", limit = 500)       // 인덱스로 좁히지 못하는 큰 기간 조회
        val searchers = (1..8).map {
            thread(name = "observe-test-search-$it") {
                while (running.get()) { runCatching { configuredStore.search(heavy) }; searches.incrementAndGet() }
            }
        }
        // 저장이 실패해 예외로 빠져도 조회 스레드를 멈춘다 — 남아 돌면 뒤 테스트의 시간을 흐린다
        val latencies = try {
            Thread.sleep(500)
            (1..40).map { index ->
                val started = System.nanoTime()
                configuredStore.insertAll(listOf(LogEntry(StreamId(now.toEpochMilli() + index, 1), "info", "backend", "req-ingest", "ingest $index")))
                Duration.ofNanos(System.nanoTime() - started).toMillis().also { Thread.sleep(50) }
            }
        } finally {
            running.set(false)
            searchers.forEach { it.join(30_000) }
        }
        println("observe-test ingest latency under 8 heavy searchers: max=${latencies.max()} ms median=${latencies.sorted()[20]} ms, searches done=${searches.get()}")
        assertTrue(searches.get() >= 8, "조회가 실제로 몰리지 않았다")
        assertTrue(latencies.max() < 300, "조회가 저장을 밀었다: $latencies")
    }
}
