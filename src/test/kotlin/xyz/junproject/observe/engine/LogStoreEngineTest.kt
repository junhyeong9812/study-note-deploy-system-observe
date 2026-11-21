package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.ObserveSchedule
import xyz.junproject.observe.api.LogController
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.FakeLogSink
import xyz.junproject.observe.collect.FakeLogStream
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogLineParser
import xyz.junproject.observe.log.LogQuery
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/** 실제 PostgreSQL — 기본 키 중복 방지, 커서 이어 보기, 30일 삭제의 경계, 검색어의 글자 그대로 찾기 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogStoreEngineTest {
    private val dataSource: HikariDataSource = EngineSupport.dataSource()
    private val store = EngineSupport.store(dataSource)
    private val jdbc = JdbcTemplate(dataSource)
    private val now = Instant.parse("2026-11-21T12:00:00Z")

    @BeforeEach fun reset() = EngineSupport.resetTable(dataSource)
    @AfterAll fun close() = dataSource.close()

    private fun rowCount() = jdbc.queryForObject("SELECT count(*) FROM log_entry", Long::class.java)!!
    private fun entry(millis: Long, sequence: Int = 0, service: String = "backend", message: String = "m-$millis-$sequence", level: String = "info") =
        LogEntry(StreamId(millis, sequence), level, service, "req-$millis", message)

    @Test
    fun `같은 항목을 두 번 저장해도 1행이다`() {
        val first = entry(now.toEpochMilli(), 3)
        assertEquals(1, store.insertAll(listOf(first)))
        assertEquals(0, store.insertAll(listOf(first.copy(message = "다시 받은 같은 항목"))), "다시 넣은 것을 새 행으로 셌다")
        assertEquals(1L, rowCount())
        assertEquals("m-${now.toEpochMilli()}-3", jdbc.queryForObject("SELECT message FROM log_entry", String::class.java))
    }

    @Test
    fun `한 묶음 안에 이미 있는 항목이 섞여도 나머지는 들어가고 묶음이 실패하지 않는다`() {
        store.insertAll(listOf(entry(1_000), entry(2_000)))
        assertEquals(1, store.insertAll(listOf(entry(1_000), entry(2_000), entry(3_000))))
        assertEquals(3L, rowCount())
    }

    @Test
    fun `콜론·줄바꿈·NUL·아주 긴 줄·빈 줄이 다른 줄과 한 묶음으로 저장된다`() {
        val base = now.toEpochMilli()
        val longContent = "가".repeat(200_000)
        val lines = listOf(
            "req-1:backend:a:b:c", "req-2:front:첫 줄\n둘째 줄\r\n셋째", "req-3:backend:널\u0000문자", "req-4:llm-wrapper:$longContent", "", "형식이 아닌 줄",
        )
        val entries = lines.mapIndexed { index, line -> LogLineParser.parse(StreamId(base, index), "info", line) }
        assertEquals(lines.size, store.insertAll(entries))
        val found = store.search(LogQuery(now.minusSeconds(1), now.plusSeconds(1), limit = 500)).associateBy { it.id.sequence }
        assertEquals(lines.size, found.size)
        assertEquals("a:b:c", found[0]!!.message)
        assertEquals("첫 줄\n둘째 줄\r\n셋째", found[1]!!.message)
        assertEquals("널문자", found[2]!!.message)
        assertEquals(longContent, found[3]!!.message, "긴 줄이 잘렸다 — 명세 ② 원문 그대로")
        assertEquals("unknown" to "", found[4]!!.service to found[4]!!.message)
        assertEquals("unknown" to "형식이 아닌 줄", found[5]!!.service to found[5]!!.message)
    }

    private val controller = run {
        val stream = FakeLogStream().also { it.ensureGroup() }
        LogController(store, CollectorHealth(LogCollector(stream, FakeLogSink()), { 0 }, { 0L }, { null }, { null }), "secret")
    }

    /** 커서를 따라 끝까지 읽는다 — 응답의 next_before 를 다음 before 로 */
    @Suppress("UNCHECKED_CAST")
    private fun readAllPages(from: Instant, to: Instant, service: String? = null, q: String? = null, limit: Int = 100): List<String> {
        val collected = mutableListOf<String>()
        var before: String? = null
        var pages = 0
        while (true) {
            val response = controller.logs(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
                from.toString(), to.toString(), service, null, null, q, before, limit.toString(), now)
            assertEquals(200, response.statusCode.value())
            val data = (response.body as Map<String, Any>)["data"] as Map<String, Any>
            collected += (data["logs"] as List<Map<String, Any>>).map { it["id"] as String }
            before = (data["next_before"] as String).ifEmpty { return collected }
            check(++pages < 1_000) { "커서가 끝나지 않는다" }
        }
    }

    @Test
    fun `커서로 끝까지 이어 보면 누락도 중복도 없이 최신순이다`() {
        val base = now.minus(Duration.ofMinutes(30)).toEpochMilli()
        // 같은 밀리초에 여러 항목(순번만 다른)이 있어 쪽 경계가 그 사이에 걸리게 한다 — 1234 건, 쪽 100 건
        val entries = (0 until 1234).map { entry(base + it / 3, it % 3, service = if (it % 2 == 0) "backend" else "front") }
        entries.chunked(500).forEach { store.insertAll(it) }
        val expected = entries.map { it.id }.sortedDescending().map { it.toString() }

        val pages = readAllPages(now.minus(Duration.ofHours(1)), now)
        assertEquals(expected.size, pages.size)
        assertEquals(expected.size, pages.toSet().size, "중복이 있다")
        assertEquals(expected, pages)

        val backendOnly = entries.filter { it.service == "backend" }.map { it.id }.sortedDescending().map { it.toString() }
        assertEquals(backendOnly, readAllPages(now.minus(Duration.ofHours(1)), now, service = "backend", limit = 7))
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `기본 창으로 받은 첫 쪽의 from·to 와 next_before 로 이어 보면 시계가 흘러도 누락·중복이 없다`() {
        // 첫 쪽은 기간 없이(최근 1시간) — 그 응답의 from·to 를 다음 쪽부터 그대로 넘긴다. 쪽 사이에 시계가 5분씩 흐르게 해
        // 창을 다시 세면 오래된 끝(처음 몇 분)이 빠지는 상황을 만든다. 창이 닫힌 뒤 들어온 로그는 섞이지 않아야 한다.
        val windowStart = now.minus(Duration.ofHours(1)).toEpochMilli()
        val entries = (0 until 1000).map { entry(windowStart + (it / 2) * 7_000L, it % 2) }   // 창의 처음부터 약 58분에 걸쳐
        val arrivedLater = (1..5).map { entry(now.toEpochMilli() + it * 60_000L) }            // 첫 쪽 뒤에 들어온 로그
        (entries + arrivedLater).chunked(500).forEach { store.insertAll(it) }
        val expected = entries.map { it.id }.sortedDescending().map { it.toString() }

        val collected = mutableListOf<String>()
        var from: String? = null
        var to: String? = null
        var before: String? = null
        var pages = 0
        while (true) {
            val clock = now.plus(Duration.ofMinutes(5L * pages))
            val response = controller.logs(MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") },
                from, to, null, null, null, null, before, "100", clock)
            assertEquals(200, response.statusCode.value(), "${response.body}")
            val data = (response.body as Map<String, Any>)["data"] as Map<String, Any>
            collected += (data["logs"] as List<Map<String, Any>>).map { it["id"] as String }
            from = data["from"] as String
            to = data["to"] as String
            before = (data["next_before"] as String).ifEmpty { null } ?: break
            check(++pages < 1_000) { "커서가 끝나지 않는다" }
        }
        assertEquals(now.minus(Duration.ofHours(1)).toString(), from, "이어 보는 동안 창이 움직였다")
        assertEquals(expected.size, collected.toSet().size, "중복이 있다")
        assertEquals(expected, collected)
    }

    @Test
    fun `가장 최근 행의 시각은 DB 에서 읽어 새 저장소로도 같고 표가 비었으면 null 이다`() {
        assertNull(store.latestStoredAt())
        val newest = entry(now.toEpochMilli(), 7)
        store.insertAll(listOf(entry(now.toEpochMilli() - 5_000), newest, entry(now.toEpochMilli(), 2)))
        // 재시작 — 메모리에 아무것도 없는 새 저장소·새 연결 풀
        EngineSupport.dataSource(size = 1).use { restarted ->
            assertEquals(now, EngineSupport.store(restarted).latestStoredAt())
        }
    }

    @Test
    fun `쪽 크기가 남은 건수와 딱 맞으면 다음 쪽이 비어 끝난다`() {
        val base = now.minusSeconds(60).toEpochMilli()
        (0 until 200).map { entry(base + it) }.let(store::insertAll)
        assertEquals(200, readAllPages(now.minus(Duration.ofHours(1)), now, limit = 100).toSet().size)
    }

    @Test
    fun `30일 삭제는 정확히 30일 된 행을 남기고 그보다 1ms 오래된 행을 지운다`() {
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), store)
        val boundary = now.minus(Duration.ofDays(30)).toEpochMilli()
        store.insertAll(listOf(
            entry(boundary - 1, message = "30일 + 1ms"),
            entry(boundary - 1, 1, message = "30일 + 1ms, 같은 밀리초 다음 순번"),
            entry(boundary, message = "정확히 30일"),
            entry(boundary + 1, message = "30일 - 1ms"),
            entry(now.toEpochMilli(), message = "지금"),
        ))
        assertNull(schedule.lastRetention)
        assertEquals(2, schedule.deleteOldLogs(now))
        assertEquals(RetentionResult(now, 2, null), schedule.lastRetention, "헬스에 낼 마지막 삭제 결과")
        assertEquals(listOf("정확히 30일", "30일 - 1ms", "지금"),
            jdbc.queryForList("SELECT message FROM log_entry ORDER BY stream_ms, stream_seq", String::class.java))
        // 두 자리가 함께 돌아도 결과가 같다
        assertEquals(0, schedule.deleteOldLogs(now))
        assertEquals(3L, rowCount())
    }

    @Test
    fun `30일 삭제가 실패하면 마지막 결과에 실패 종류가 남는다`() {
        val closed = EngineSupport.dataSource(size = 1).also { it.close() }
        val schedule = ObserveSchedule(LogCollector(FakeLogStream(), FakeLogSink()), EngineSupport.store(closed))
        assertNull(schedule.deleteOldLogs(now))
        val result = schedule.lastRetention!!
        assertEquals(now to null, result.ranAt to result.deleted)
        assertEquals("CannotGetJdbcConnectionException", result.failure)
    }

    @Test
    fun `퍼센트·밑줄·역슬래시가 든 검색어는 글자 그대로 찾는다`() {
        val base = now.minusSeconds(60).toEpochMilli()
        val messages = listOf("disk 100% full", "disk 1000 full", "user_id missing", "userXid missing", "path C:\\temp\\x", "path C:/temp/x", "a\\%b literal")
        store.insertAll(messages.mapIndexed { index, message -> entry(base + index, message = message) })
        fun find(text: String) = store.search(LogQuery(now.minus(Duration.ofHours(1)), now, text = text, limit = 500)).map { it.message }.toSet()
        assertEquals(setOf("disk 100% full"), find("0% f"))
        assertEquals(setOf("user_id missing"), find("r_id"))
        assertEquals(setOf("path C:\\temp\\x"), find(":\\temp"))
        assertEquals(setOf("a\\%b literal"), find("a\\%b"))
        assertEquals(emptySet<String>(), find("%%"))
        assertEquals(emptySet<String>(), find("__"))
    }

    @Test
    fun `서비스·레벨별 시간 단위 건수`() {
        val hour = Instant.parse("2026-11-21T10:00:00Z").toEpochMilli()
        store.insertAll(listOf(
            entry(hour + 1, level = "error"), entry(hour + 2, level = "error"), entry(hour + 3, service = "front"),
            entry(hour + 3_600_000, level = "error"),
        ))
        val counts = store.stats(Instant.parse("2026-11-21T09:00:00Z"), Instant.parse("2026-11-21T12:00:00Z"))
            .map { "${it.hour} ${it.service} ${it.level} ${it.count}" }
        assertEquals(listOf(
            "2026-11-21T10:00:00Z backend error 2", "2026-11-21T10:00:00Z front info 1", "2026-11-21T11:00:00Z backend error 1",
        ), counts)
    }
}
