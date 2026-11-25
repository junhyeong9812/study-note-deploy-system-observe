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
import xyz.junproject.observe.traffic.TrafficCell
import xyz.junproject.observe.traffic.TrafficCellQuery
import xyz.junproject.observe.traffic.TrafficClass
import xyz.junproject.observe.traffic.TrafficDeletion
import xyz.junproject.observe.traffic.TrafficEntrySummary
import xyz.junproject.observe.traffic.TrafficRecords
import xyz.junproject.observe.traffic.TrafficServerProgress
import xyz.junproject.observe.traffic.TrafficStatusCounts
import xyz.junproject.observe.traffic.TrafficStore
import xyz.junproject.observe.traffic.TrafficStoreOutcome
import xyz.junproject.observe.traffic.TrafficTotals
import java.time.Duration
import java.time.Instant

/**
 * 실제 PostgreSQL — 규약 4.5 의 저장 규칙: 순번으로 통째로 버림 · 추가만 하는 원본 행과 읽을 때의 합 · 칸마다의 앞섬 2분 · 오래됨 6일(검사의 차례는 순번 먼저) ·
 * `at` 으로 버리지 않음 · 순번은 버린 칸이 있어도 "받음" · 한 항목 = 한 트랜잭션 · 순번의 틈과 읽기 진도(세대마다) · `missing` · 요청 구간 ·
 * 5분 묶음의 다시 계산 · 보관의 경계 · 저장된 것 어디에도 주소 · 쿼리가 없음. 표는 전용 스키마(`traffic_store`).
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TrafficStoreEngineTest {
    private val schema = "traffic_store"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@TrafficStoreEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 4
    })
    private val jdbc = JdbcTemplate(dataSource)
    private val store = TrafficStore(jdbc, jdbc, jdbc)
    /** 30초 경계에 맞춘 지금 — 칸 시작이 경계에 닿는 사례를 만들 수 있게 */
    private val now = Instant.parse("2026-11-25T04:00:00Z")

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

    private fun body(vararg segments: String, requests: List<String> = emptyList(), missing: List<String> = emptyList(), requestsDropped: Long = 0) =
        TrafficRecords.body(segments.toList(), requests, missing, requestsDropped)

    private fun store(sequence: Long, body: String = body(), at: Instant = now.minusSeconds(1), receivedAt: Instant = now,
                      generation: String = TrafficRecords.GENERATION, server: String = TrafficRecords.SERVER): TrafficStoreOutcome =
        store.store(TrafficRecords.entry(sequence, at, receivedAt, body, server, generation))

    private fun cells(from: Instant = now.minus(Duration.ofDays(8)), to: Instant = now.plus(Duration.ofHours(1)), rawSince: Instant = from): List<TrafficCell> =
        store.cells(TrafficCellQuery(from, to, rawSince))

    private fun totalsAt(start: Instant, service: String = "backend", caller: String = "front", server: String = TrafficRecords.SERVER) =
        cells().single { it.bucketStart == start && it.service == service && it.caller == caller && it.server == server }.totals

    private fun buckets(vararg pairs: Pair<Int, Long>) = LongArray(20).also { array -> pairs.forEach { (index, value) -> array[index] = value } }.toList()

    private fun progressOf(server: String = TrafficRecords.SERVER): TrafficServerProgress = store.serverProgress().single { it.server == server }

    @Test
    fun `같은 서버 · 세대 · 순번이 두 번 오면 두 번째는 통째로 버려지고 칸의 합과 요청별 줄이 변하지 않는다`() {
        val cell = now.minusSeconds(30)
        val first = store(1, body(TrafficRecords.segment(cell, listOf(200, 200, 500)), requests = listOf(TrafficRecords.request(cell.plusSeconds(1), requestId = "req-first"))))
        assertEquals(TrafficStoreOutcome(false, segmentsStored = 1, requestsStored = 1), first)
        val again = store(1, body(TrafficRecords.segment(cell, listOf(200, 200, 200, 200, 200)), requests = listOf(TrafficRecords.request(cell.plusSeconds(2), requestId = "req-second"))),
            receivedAt = now.plusSeconds(5))
        assertEquals(TrafficStoreOutcome(duplicate = true), again)
        assertEquals(TrafficTotals(3, TrafficStatusCounts(2, 0, 0, 1, 0), buckets(3 to 3L), 40), totalsAt(cell))
        assertEquals(1, store.requestSpans("req-first", 10).size)
        assertEquals(0, store.requestSpans("req-second", 10).size, "버린 항목의 요청별 줄이 들어갔다")
        assertEquals(now, progressOf().lastReceivedAt, "버린 항목을 수신으로 쳤다")
    }

    @Test
    fun `열쇠는 서버 · 세대 · 순번 셋이다 — 세대나 서버가 다르면 같은 순번도 따로 받는다`() {
        val cell = now.minusSeconds(30)
        assertFalse(store(1, body(TrafficRecords.segment(cell, listOf(200)))).duplicate)
        assertFalse(store(1, body(TrafficRecords.segment(cell, listOf(200))), generation = TrafficRecords.OTHER_GENERATION).duplicate)
        assertFalse(store(1, body(TrafficRecords.segment(cell, listOf(200))), server = "ci-agent-164").duplicate)
        assertEquals(2L, totalsAt(cell).count)
        assertEquals(1L, totalsAt(cell, server = "ci-agent-164").count)
    }

    @Test
    fun `한 칸이 여러 항목에 걸쳐 채워지면 수 · 상태 · 분포는 더하고 max_ms 는 큰 쪽이며 원본 행은 항목마다 추가된다`() {
        val cell = now.minus(Duration.ofMinutes(3))
        store(1, body(TrafficRecords.segment(cell, listOf(200, 404), durationsMs = listOf(3, 260))))
        store(2, body(TrafficRecords.segment(cell, listOf(503), durationsMs = listOf(190_000)), TrafficRecords.segment(cell, listOf(200), caller = "admin")))
        store(3, body(TrafficRecords.segment(cell, listOf(302, 200, 200), durationsMs = listOf(9, 4, 30))))
        assertEquals(
            TrafficTotals(6, TrafficStatusCounts(3, 1, 1, 1, 0), buckets(0 to 2L, 1 to 1L, 3 to 1L, 6 to 1L, 19 to 1L), 190_000),
            totalsAt(cell),
        )
        assertEquals(1L, totalsAt(cell, caller = "admin").count, "다른 호출자의 칸과 섞였다")
        val rows = administrationJdbc.queryForObject(
            "SELECT count(*) FROM $schema.traffic_segment WHERE bucket_ms = ? AND caller = 'front'", Long::class.java, cell.toEpochMilli())
        assertEquals(3L, rows, "칸 한 줄을 고쳐 더했다 — 원본 행은 항목마다 추가만 한다")
    }

    @Test
    fun `2분 넘게 앞선 칸 · 6일 넘게 오래된 칸은 버리고 세며 같은 항목의 다른 칸과 정확히 경계인 칸은 들어간다`() {
        val aheadEdge = now.plus(Duration.ofMinutes(2))
        val ahead = aheadEdge.plusSeconds(30)
        val oldEdge = now.minus(Duration.ofDays(6))
        val old = oldEdge.minusSeconds(30)
        val normal = now.minusSeconds(30)
        val outcome = store(1, body(
            TrafficRecords.segment(aheadEdge, listOf(200)), TrafficRecords.segment(ahead, listOf(200)),
            TrafficRecords.segment(oldEdge, listOf(200)), TrafficRecords.segment(old, listOf(200)), TrafficRecords.segment(normal, listOf(200)),
            requests = listOf(
                TrafficRecords.request(aheadEdge, requestId = "req-edge-ahead"), TrafficRecords.request(aheadEdge.plusMillis(1), requestId = "req-ahead"),
                TrafficRecords.request(oldEdge, requestId = "req-edge-old"), TrafficRecords.request(oldEdge.minusMillis(1), requestId = "req-old"),
            ),
        ))
        assertEquals(TrafficStoreOutcome(false, segmentsStored = 3, segmentsAhead = 1, segmentsTooOld = 1, requestsStored = 2, requestsAhead = 1, requestsTooOld = 1), outcome)
        assertEquals(listOf(oldEdge, normal, aheadEdge), cells(from = now.minus(Duration.ofDays(7))).map { it.bucketStart })
        assertEquals(1, store.requestSpans("req-edge-ahead", 10).size)
        assertEquals(1, store.requestSpans("req-edge-old", 10).size)
        assertEquals(0, store.requestSpans("req-ahead", 10).size)
        assertEquals(0, store.requestSpans("req-old", 10).size)
    }

    @Test
    fun `칸을 모두 버린 항목도 순번은 받음으로 적혀 다시 오면 통째로 버리고 틈으로 세지 않는다`() {
        val old = now.minus(Duration.ofDays(9))
        assertEquals(TrafficStoreOutcome(false, segmentsTooOld = 1, requestsTooOld = 1),
            store(1, body(TrafficRecords.segment(old, listOf(200)), requests = listOf(TrafficRecords.request(old)))))
        assertEquals(TrafficStoreOutcome(duplicate = true), store(1, body(TrafficRecords.segment(now.minusSeconds(30), listOf(200)))))
        store(2)
        assertEquals(0L, progressOf().sequenceGaps)
        assertEquals(emptyList<TrafficCell>(), cells(from = now.minus(Duration.ofDays(10))))
    }

    @Test
    fun `검사의 차례는 순번이 먼저다 — 이미 받은 순번은 칸이 오래됐어도 칸으로 세지 않고 순번으로 버린다`() {
        store(1)
        val old = now.minus(Duration.ofDays(9))
        assertEquals(TrafficStoreOutcome(duplicate = true), store(1, body(TrafficRecords.segment(old, listOf(200)), requests = listOf(TrafficRecords.request(old)))))
    }

    @Test
    fun `항목의 at 이 아무리 앞서거나 오래돼도 항목을 버리지 않는다`() {
        val cell = now.minusSeconds(30)
        assertEquals(1, store(1, body(TrafficRecords.segment(cell, listOf(200))), at = now.plus(Duration.ofHours(3))).segmentsStored)
        assertEquals(1, store(2, body(TrafficRecords.segment(cell, listOf(200))), at = now.minus(Duration.ofDays(30))).segmentsStored)
        assertEquals(2L, totalsAt(cell).count)
    }

    @Test
    fun `순번 1 · 2 · 4 면 틈 1 이고 읽기 진도는 2 의 at · 3 이 늦게 오면 틈 0 이고 진도는 4 의 at · 다시 온 순번은 아무것도 바꾸지 않는다`() {
        val at1 = now.minusSeconds(95); val at2 = now.minusSeconds(65); val at3 = now.minusSeconds(35); val at4 = now.minusSeconds(5)
        store(1, at = at1, receivedAt = now.minusSeconds(90))
        store(2, at = at2, receivedAt = now.minusSeconds(60))
        store(4, at = at4, receivedAt = now)
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, at2, now.minusSeconds(60), 1, now, 1), progressOf(), "틈 뒤의 항목으로 진도를 올렸다")
        store(3, at = at3, receivedAt = now.plusSeconds(10))
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, at4, now.plusSeconds(10), 0, now.plusSeconds(10), 1), progressOf(), "늦게 와서 메워진 틈을 빼지 않았다")
        store(2, at = at2, receivedAt = now.plusSeconds(20))
        store(4, at = at4, receivedAt = now.plusSeconds(20))
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, at4, now.plusSeconds(10), 0, now.plusSeconds(10), 1), progressOf(), "다시 온 순번이 틈 · 진도 · 수신을 바꿨다")
    }

    @Test
    fun `세대가 바뀌면 새 세대는 따로 세고 옛 세대의 틈은 사라지지 않는다`() {
        for (sequence in listOf(1L, 2, 4)) store(sequence, at = now.minusSeconds(200 - sequence), receivedAt = now.minusSeconds(100))
        for (sequence in listOf(1L, 2, 3, 5)) store(sequence, at = now.minusSeconds(50 - sequence), receivedAt = now, generation = TrafficRecords.OTHER_GENERATION)
        val progress = progressOf()
        assertEquals(2L, progress.sequenceGaps, "세대를 섞어 셌다")
        assertEquals(now.minusSeconds(47), progress.readProgressAt, "새 세대의 틈 없이 이어진 끝(3)의 at 이 아니다")
        assertEquals(listOf(TrafficRecords.SERVER), store.serverProgress().map { it.server })
    }

    @Test
    fun `틈난 순번은 뒤의 순번을 처음 받은 때부터 10분 넘게 오지 않으면 잃음으로 확정되고 진도는 틈을 넘으며 정확히 10분은 아직 기다린다`() {
        val at2 = now.minusSeconds(65)
        val firstLater = now                                                   // 순번 4 를 받은 때 — 3 을 기다리는 창의 시작
        store(1, at = now.minusSeconds(95), receivedAt = now.minusSeconds(90))
        store(2, at = at2, receivedAt = now.minusSeconds(60))
        store(4, at = now.minusSeconds(5), receivedAt = firstLater)
        store(5, at = now.plus(Duration.ofMinutes(10)).minusSeconds(5), receivedAt = firstLater.plus(Duration.ofMinutes(10)))
        assertEquals(at2, progressOf().readProgressAt, "10분이 되기 전에 틈을 넘었다")
        assertEquals(now.minusSeconds(60), progressOf().progressAdvancedAt)
        assertTrue(xyz.junproject.observe.traffic.TrafficReadProgress.isReceiveDelayed(progressOf().progressAdvancedAt, firstLater.plus(Duration.ofMinutes(10))))
        val at6 = now.plus(Duration.ofMinutes(10)).plusSeconds(25)
        val confirmedAt = firstLater.plus(Duration.ofMinutes(10)).plusMillis(1)
        store(6, at = at6, receivedAt = confirmedAt)
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, at6, confirmedAt, 1, confirmedAt, 1), progressOf(), "확정된 틈을 넘어 진도가 나아가지 않았다")
        assertFalse(xyz.junproject.observe.traffic.TrafficReadProgress.isReceiveDelayed(progressOf().progressAdvancedAt, confirmedAt.plusSeconds(1)), "확정 뒤에도 수신 지연이다")
        // 확정 뒤 늦게 온 3 — 저장하고 틈은 하나 줄지만 진도는 이미 지나갔다
        val lateAt = confirmedAt.plusSeconds(30)
        assertEquals(1, store(3, body(TrafficRecords.segment(now.minusSeconds(60), listOf(200))), at = now.minusSeconds(35), receivedAt = lateAt).segmentsStored)
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, at6, confirmedAt, 0, lateAt, 1), progressOf())
    }

    @Test
    fun `틈 기다림은 저장소에 주는 값을 따른다`() {
        val shortWait = TrafficStore(jdbc, jdbc, jdbc, gapWait = Duration.ofMinutes(1))
        shortWait.store(TrafficRecords.entry(1, now.minusSeconds(90), now.minusSeconds(90)))
        shortWait.store(TrafficRecords.entry(3, now.minusSeconds(80), now.minusSeconds(80)))
        shortWait.store(TrafficRecords.entry(4, now.minusSeconds(10), now.minusSeconds(80).plus(Duration.ofMinutes(1)).plusMillis(1)))
        assertEquals(now.minusSeconds(10), progressOf().readProgressAt)
        assertEquals(1L, progressOf().sequenceGaps)
    }

    @Test
    fun `세대에서 처음 본 순번이 1 이 아니면 그 순번이 시작이다 — 앞은 틈이 아니고 first_seq_seen 으로 드러난다`() {
        store(5, at = now.minusSeconds(40), receivedAt = now.minusSeconds(40))
        store(6, at = now.minusSeconds(10), receivedAt = now.minusSeconds(10))
        assertEquals(TrafficServerProgress(TrafficRecords.SERVER, now.minusSeconds(10), now.minusSeconds(10), 0, now.minusSeconds(10), 5), progressOf())
        store(8, at = now.minusSeconds(5), receivedAt = now.minusSeconds(5))
        assertEquals(1L, progressOf().sequenceGaps, "처음 본 순번 뒤의 틈을 세지 않았다")
        // 새 세대가 오면 first_seq_seen 은 가장 최근 세대의 것
        store(1, at = now, receivedAt = now, generation = TrafficRecords.OTHER_GENERATION)
        assertEquals(1L, progressOf().firstSeqSeen)
    }

    @Test
    fun `받은 열쇠가 7일 뒤 지워진 순번이 다시 와도 틈의 수는 0 아래로 내려가지 않는다`() {
        val old = now.minus(Duration.ofDays(8))
        store(1, at = old, receivedAt = old)
        store(2, at = old.plusSeconds(30), receivedAt = old.plusSeconds(30))
        assertEquals(2, store.deleteExpired(now).entriesDeleted)
        assertFalse(store(1, at = old, receivedAt = now).duplicate)
        assertEquals(0L, progressOf().sequenceGaps)
    }

    @Test
    fun `쿼리 · 주소 꼴 경로의 줄은 그 줄만 버려 세고 묶음과 다른 줄은 저장된다`() {
        val cell = now.minusSeconds(30)
        val outcome = store(1, body(TrafficRecords.segment(cell, listOf(200, 200, 200, 200)), requests = listOf(
            TrafficRecords.request(cell, requestId = "req-good-1"), TrafficRecords.request(cell, requestId = "req-good-2", path = "/api/search"),
            TrafficRecords.request(cell, requestId = "req-query", path = "/search?q=MARKER-q"), TrafficRecords.request(cell, requestId = "req-ip", path = "/ip/198.51.100.7"))))
        assertEquals(2, outcome.requestsStored)
        assertEquals(4L, totalsAt(cell).count)
        assertEquals(listOf(1, 1, 0, 0), listOf("req-good-1", "req-good-2", "req-query", "req-ip").map { store.requestSpans(it, 10).size })
        assertEquals(2L, store.entrySummaries(now.minusSeconds(1), now.plusSeconds(1)).getValue(TrafficRecords.SERVER).requestsRejected)
    }

    @Test
    fun `서버마다 따로 센다 — 한 서버의 틈과 진도가 다른 서버에 섞이지 않는다`() {
        store(1, at = now.minusSeconds(40))
        store(3, at = now.minusSeconds(10))
        store(1, at = now.minusSeconds(20), server = "ci-agent-164")
        assertEquals(listOf("ci-agent-158", "ci-agent-164"), store.serverProgress().map { it.server })
        assertEquals(1L, progressOf().sequenceGaps)
        assertEquals(0L, progressOf("ci-agent-164").sequenceGaps)
        assertEquals(now.minusSeconds(20), progressOf("ci-agent-164").readProgressAt)
    }

    @Test
    fun `missing 과 상한으로 싣지 못한 요청 수를 저장해 받은 기간의 요약으로 낸다`() {
        store(1, body(missing = listOf("backend-nginx:rotated"), requestsDropped = 4), receivedAt = now.minusSeconds(60))
        store(2, body(missing = listOf("backend-nginx:rotated", "ci-cd-agent:cursor_lost"), requestsDropped = 3), receivedAt = now.minusSeconds(30))
        store(3, body(missing = listOf("front-nginx:replaced")), receivedAt = now.minus(Duration.ofHours(2)))
        assertEquals(
            mapOf(TrafficRecords.SERVER to TrafficEntrySummary(listOf("backend-nginx:rotated", "ci-cd-agent:cursor_lost"), 7)),
            store.entrySummaries(now.minus(Duration.ofMinutes(5)), now),
        )
    }

    @Test
    fun `요청 구간은 요청 ID 하나의 줄들을 end 순으로 서버와 함께 내고 상한까지만`() {
        val end = now.minusSeconds(20)
        store(1, body(requests = listOf(
            TrafficRecords.request(end.plusMillis(30), requestId = "req-trace", service = "backend", caller = "front", durationMs = 25, upstreamMs = 24, slot = "green"),
            TrafficRecords.request(end, requestId = "req-other"),
        )))
        store(1, body(requests = listOf(TrafficRecords.request(end, requestId = "req-trace", service = "front", caller = "edge", durationMs = 60, upstreamMs = null, upstreamStatus = null, slot = null))),
            server = "ci-agent-164")
        val spans = store.requestSpans("req-trace", 10)
        assertEquals(listOf("ci-agent-164" to "front", "ci-agent-158" to "backend"), spans.map { it.server to it.request.service })
        assertEquals(null, spans[0].request.upstreamMs)
        assertEquals("green", spans[1].request.slot)
        assertEquals(end.plusMillis(30), spans[1].request.endAt)
        assertEquals(1, store.requestSpans("req-trace", 1).size)
    }

    @Test
    fun `한 항목의 저장은 한 트랜잭션이다 — 요청별 줄이 실패하면 열쇠도 칸도 남지 않고 다시 받으면 한 번 들어간다`() {
        val cell = now.minusSeconds(30)
        val entryBody = body(TrafficRecords.segment(cell, listOf(200)), requests = listOf(TrafficRecords.request(cell)))
        administrationJdbc.execute("""
            CREATE FUNCTION $schema.observe_test_refuse() RETURNS trigger LANGUAGE plpgsql AS ${'$'}${'$'}
            BEGIN RAISE EXCEPTION 'refused by test'; END ${'$'}${'$'}""")
        administrationJdbc.execute("CREATE TRIGGER observe_test_refuse BEFORE INSERT ON $schema.traffic_request FOR EACH ROW EXECUTE FUNCTION $schema.observe_test_refuse()")
        assertThrows(Exception::class.java) { store(1, entryBody) }
        assertEquals(emptyList<TrafficCell>(), cells(), "실패한 항목의 칸이 남았다")
        assertEquals(emptyList<TrafficServerProgress>(), store.serverProgress(), "실패한 항목의 열쇠가 남았다")
        administrationJdbc.execute("DROP TRIGGER observe_test_refuse ON $schema.traffic_request")
        assertFalse(store(1, entryBody).duplicate, "실패한 항목의 열쇠가 남아 다시 받은 항목을 버렸다")
        assertEquals(1L, totalsAt(cell).count)
    }

    @Test
    fun `5분 묶음은 원본에서 만들어지고 늦게 저장된 항목이 그 구간을 바꾸면 다음 바퀴에 다시 계산된다`() {
        val bucket = now.minus(Duration.ofDays(8))                              // 5분 경계
        val received = bucket.plusSeconds(40)
        store(1, body(TrafficRecords.segment(bucket, listOf(200, 500), durationsMs = listOf(3, 70))), at = received, receivedAt = received)
        store(2, body(TrafficRecords.segment(bucket.plusSeconds(60), listOf(200), durationsMs = listOf(30))), at = received.plusSeconds(60), receivedAt = received.plusSeconds(60))
        val firstRun = bucket.plus(Duration.ofHours(1))
        assertEquals(1, store.rollUp(firstRun))
        fun rollup() = store.cells(TrafficCellQuery(bucket, bucket.plus(Duration.ofMinutes(5)), rawSince = now)).single()
        assertEquals(Duration.ofMinutes(5), rollup().width)
        assertEquals(bucket, rollup().bucketStart)
        assertEquals(TrafficTotals(3, TrafficStatusCounts(2, 0, 0, 1, 0), buckets(0 to 1L, 3 to 1L, 4 to 1L), 70), rollup().totals)
        // 다른 자리가 계산 전에 받아 두었다가 계산 뒤에 저장한 항목 — 받은 시각은 계산 시각보다 앞이다
        store(3, body(TrafficRecords.segment(bucket.plusSeconds(120), listOf(404), durationsMs = listOf(900))), at = received, receivedAt = firstRun.minusSeconds(10))
        assertEquals(1, store.rollUp(firstRun.plusSeconds(60)), "늦게 저장된 항목이 있는 구간을 다시 계산하지 않았다")
        assertEquals(TrafficTotals(4, TrafficStatusCounts(2, 0, 1, 1, 0), buckets(0 to 1L, 3 to 1L, 4 to 1L, 7 to 1L), 900), rollup().totals)
        assertEquals(0, store.rollUp(firstRun.plusSeconds(120)), "여러 번 돌려도 같아야 한다")
    }

    @Test
    fun `보관 — 원본 칸은 7일 뒤 5분 묶음이 다 담은 구간만 지우고 요청별 줄 · 받은 열쇠는 7일 · 5분 묶음은 30일 · 경계는 남는다`() {
        val cutoff = now.minus(Duration.ofDays(7))                              // 5분 경계
        val oldBucket = cutoff.minus(Duration.ofMinutes(5))
        // 원본 칸: 경계보다 한 칸 앞(지운다) · 정확히 경계(남긴다) — 받은 시각은 칸과 같은 때
        store(1, body(TrafficRecords.segment(oldBucket, listOf(200)), TrafficRecords.segment(oldBucket.plusSeconds(270), listOf(200)),
            requests = listOf(TrafficRecords.request(cutoff.minusMillis(1), requestId = "req-expired"), TrafficRecords.request(cutoff, requestId = "req-edge"))),
            at = oldBucket.plusSeconds(1), receivedAt = cutoff.minusMillis(1))
        store(2, body(TrafficRecords.segment(cutoff, listOf(200))), at = cutoff, receivedAt = cutoff)
        val unrolled = cutoff.minus(Duration.ofMinutes(10))
        store.rollUp(cutoff.plusSeconds(1))
        // 5분 묶음이 아직 없는 구간 — 지우지 않는다
        store(3, body(TrafficRecords.segment(unrolled, listOf(200))), at = unrolled, receivedAt = cutoff.plusSeconds(2))
        val deletion = store.deleteExpired(now)
        assertEquals(TrafficDeletion(entriesDeleted = 1, segmentsDeleted = 2, requestsDeleted = 1, rollupDeleted = 0), deletion)
        assertEquals(listOf(unrolled, cutoff), cells(from = cutoff.minus(Duration.ofHours(1))).map { it.bucketStart }, "평균이 없는 구간을 지웠거나 경계의 칸을 지웠다")
        assertEquals(0, store.requestSpans("req-expired", 10).size)
        assertEquals(1, store.requestSpans("req-edge", 10).size)
        // 받은 열쇠가 지워졌으므로 순번 1 은 다시 받을 수 있다 — 칸은 오래돼 들어가지 않는다(6일 검사가 막는다)
        assertEquals(TrafficStoreOutcome(false, segmentsTooOld = 1), store(1, body(TrafficRecords.segment(oldBucket, listOf(200))), at = oldBucket))
        // 5분 묶음은 30일 — 구간 시작이 정확히 30일 전이면 남는다
        val rollupEdge = now.minus(Duration.ofDays(30))
        store(10, body(TrafficRecords.segment(rollupEdge, listOf(200)), TrafficRecords.segment(rollupEdge.minus(Duration.ofMinutes(5)), listOf(200))),
            at = rollupEdge, receivedAt = rollupEdge.plusSeconds(1), generation = TrafficRecords.OTHER_GENERATION)
        store.rollUp(now)
        val rollups = store.deleteExpired(now).rollupDeleted
        assertEquals(1, rollups)
        assertEquals(listOf(rollupEdge), store.cells(TrafficCellQuery(rollupEdge.minus(Duration.ofMinutes(20)), rollupEdge.plus(Duration.ofMinutes(5)), rawSince = now)).map { it.bucketStart })
    }

    @Test
    fun `저장되는 것 어디에도 주소 · 쿼리 · 모르는 칸의 값이 없다`() {
        val cell = now.minusSeconds(30)
        val marked = TrafficRecords.body(
            segments = listOf(TrafficRecords.segment(cell, listOf(200), extra = ", \"client\": \"203.0.113.9\"")),
            requests = listOf(TrafficRecords.request(cell, path = "/api/doc", extra = ", \"remote_addr\": \"203.0.113.9\", \"query\": \"q=MARKER-query\", \"x_forwarded_for\": \"198.51.100.7\"")),
            extra = ", \"remote_addr\": \"203.0.113.9\", \"referer\": \"MARKER-referer\"",
        )
        store(1, marked)
        val tables = administrationJdbc.queryForList(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE 'traffic%' ORDER BY 1", String::class.java, schema)
        assertTrue(tables.containsAll(listOf("traffic_receipt", "traffic_segment", "traffic_request")), "표가 계약의 이름이 아니다: $tables")
        val stored = tables.flatMap { table -> administrationJdbc.queryForList("SELECT row_to_json(t)::text FROM $schema.$table t", String::class.java) }.joinToString("\n")
        assertTrue(stored.contains("/api/doc"), "맞는 줄의 경로가 저장되지 않았다")
        for (marker in listOf("203.0.113.9", "198.51.100.7", "MARKER")) assertFalse(stored.contains(marker), "저장된 행에 $marker 가 있다")
    }
}
