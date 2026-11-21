package xyz.junproject.observe.collect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.BadSqlGrammarException
import xyz.junproject.observe.log.RetentionResult
import java.sql.SQLException
import java.time.Duration
import java.time.Instant

/** 명세 ②: /health — 마지막으로 저장한 로그의 시각, 아직 확인하지 못한 건수. DB 나 Redis 에 닿지 못하면 503. 200 은 ok 하나뿐. */
class CollectorHealthTest {
    private val stream = FakeLogStream()
    private val sink = FakeLogSink()
    private val startedAt = Instant.parse("2026-11-21T00:00:00Z")
    private var now = startedAt
    private var databaseReachable = true
    private var tableMissing = false                                  // 스키마가 아직 없다 — 실제 DB 는 SQLSTATE 42P01 로 답한다
    private var latestInDatabase: Instant? = null
    private var lag: () -> Long? = { 0L }
    private var retention: RetentionResult? = null
    private val collector = LogCollector(stream, sink, block = Duration.ZERO, clock = { now })
    private val health = CollectorHealth(
        collector,
        unacknowledged = { stream.unacknowledgedCount() },
        lag = { lag() },
        latestStoredAt = {
            if (!databaseReachable) throw IllegalStateException("DB 에 닿지 못함 jdbc:postgresql://postgres:5432")
            if (tableMissing) throw BadSqlGrammarException("latest", "SELECT stream_ms FROM log_entry",
                SQLException("ERROR: relation \"log_entry\" does not exist", "42P01"))
            latestInDatabase
        },
        lastRetention = { retention },
        startedAt = startedAt,
    )

    private fun check() = health.report(now).let { (if (it.healthy) 200 else 503) to it.body }

    @Test
    fun `방금 한 바퀴를 돌았으면 200 이고 마지막 저장 시각과 미확인 건수를 보인다`() {
        stream.add(StreamRecord("1763700000123-0", "info", "req-1:backend:x"))
        collector.runOnce()
        latestInDatabase = Instant.ofEpochMilli(1_763_700_000_123)
        val (status, body) = check()
        assertEquals(200, status)
        assertEquals("ok", body["status"])
        assertEquals("2025-11-21T04:40:00.123Z", body["last_stored_at"])
        assertEquals(0L, body["unacknowledged"])
    }

    @Test
    fun `저장에 실패해 미확인이 쌓이면 503 이고 그 건수를 보인다`() {
        stream.add(StreamRecord("1-0", "info", "a"), StreamRecord("2-0", "info", "b"))
        sink.failing = true
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503, status)
        assertEquals(2L, body["unacknowledged"])
    }

    @Test
    fun `읽을 것이 없어 저장을 안 하는 동안에도 DB 에 닿지 못하면 503`() {
        collector.runOnce()                                  // 스트림이 조용하다 — 바퀴는 성공
        databaseReachable = false
        assertEquals(503, check().first, "DB 가 죽었는데 조용한 스트림 때문에 정상으로 보였다")
    }

    @Test
    fun `Redis 에 닿지 못하면 503`() {
        collector.runOnce()
        stream.reachable = false
        assertEquals(503, check().first)
    }

    @Test
    fun `첫 바퀴가 성공하기 전에는 starting 이어도 503 이고 1분이 지나면 stalled`() {
        // 배포의 헬스 게이트는 첫 200 에서 통과한다 — 한 번도 수집하지 못한 자리를 통과시키지 않게 첫 성공 전에는 503
        stream.ensureGroup()
        assertEquals(503 to "starting", check().let { it.first to it.second["status"] })
        now = startedAt.plusSeconds(120)
        assertEquals(503 to "stalled", check().let { it.first to it.second["status"] })
    }

    @Test
    fun `첫 바퀴가 성공하면 그때부터 200`() {
        assertEquals(503, check().first)
        collector.runOnce()
        assertEquals(200 to "ok", check().let { it.first to it.second["status"] })
    }

    @Test
    fun `헬스 본문에는 예외 문구 없이 종류 이름만 낸다`() {
        // 헬스는 인증이 없다 — 접속 주소·SQL 같은 예외 문구는 서버 로그에만 남긴다
        collector.runOnce()
        databaseReachable = false
        val (status, body) = check()
        assertEquals(503, status)
        assertEquals("database IllegalStateException", body["failure"])
    }

    @Test
    fun `수집 바퀴의 실패 사유도 종류 이름만 낸다`() {
        stream.add(StreamRecord("1-0", "info", "a"))
        sink.failing = true
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503 to "failing", status to body["status"])
        assertEquals("IllegalStateException", body["failure"])
    }

    @Test
    fun `마지막 바퀴가 1분 넘게 전이면 멈춘 것으로 보고 503`() {
        collector.runOnce()
        now = now.plusSeconds(120)
        assertEquals(503 to "stalled", check().let { it.first to it.second["status"] })
    }

    @Test
    fun `마지막 저장 시각은 DB 에서 읽어 재시작 직후 이 프로세스가 아무것도 저장하지 않았어도 보인다`() {
        latestInDatabase = Instant.parse("2026-11-20T23:59:59.500Z")    // 앞 프로세스(또는 다른 자리)가 저장한 행
        collector.runOnce()                                            // 스트림이 조용하다 — 이 프로세스는 아무것도 저장하지 않았다
        val (status, body) = check()
        assertEquals(200, status)
        assertEquals(0L, body["stored_since_start"])
        assertEquals("2026-11-20T23:59:59.500Z", body["last_stored_at"])
    }

    @Test
    fun `표가 비었으면 마지막 저장 시각은 빈 문자열이고 그것만으로 503 이 되지 않는다`() {
        collector.runOnce()
        assertEquals(200 to "", check().let { it.first to it.second["last_stored_at"] })
    }

    @Test
    fun `밀린 건수를 보이되 판정에는 넣지 않고 읽지 못하면 -1`() {
        collector.runOnce()
        lag = { 9_000L }
        assertEquals(200 to 9_000L, check().let { it.first to it.second["lag"] })
        lag = { null }                                                   // Redis 가 셀 수 없다고 했다
        assertEquals(200 to -1L, check().let { it.first to it.second["lag"] })
        lag = { throw IllegalStateException("ERR unknown subcommand") }
        assertEquals(200 to -1L, check().let { it.first to it.second["lag"] })
    }

    @Test
    fun `30일 삭제의 마지막 실행 시각과 결과를 보이되 실패해도 판정에는 넣지 않는다`() {
        collector.runOnce()
        check().second.let {
            assertEquals("", it["last_retention_at"])
            assertEquals(-1, it["last_retention_deleted"])
            assertEquals("", it["last_retention_failure"])
        }
        retention = RetentionResult(Instant.parse("2026-11-21T03:40:00Z"), 812, null)
        check().second.let {
            assertEquals("2026-11-21T03:40:00Z", it["last_retention_at"])
            assertEquals(812, it["last_retention_deleted"])
            assertEquals("", it["last_retention_failure"])
        }
        retention = RetentionResult(Instant.parse("2026-11-22T03:40:00Z"), null, "QueryTimeoutException")
        val (status, body) = check()
        assertEquals(200, status)
        assertEquals("2026-11-22T03:40:00Z", body["last_retention_at"])
        assertEquals(-1, body["last_retention_deleted"])
        assertEquals("QueryTimeoutException", body["last_retention_failure"])
    }

    @Test
    fun `스키마를 만들지 못하면 503 failing 이고 사유에 schema 가 남는다`() {
        sink.schemaFailing = true
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503 to "failing", status to body["status"])
        assertEquals("schema IllegalStateException", body["failure"])
    }

    @Test
    fun `DB 확인 결과를 담아 두지 않아 끊긴 다음 요청에서 바로 503 이고 이어지면 바로 200`() {
        collector.runOnce()
        assertEquals(200, check().first)
        databaseReachable = false
        assertEquals(503 to "unreachable", check().let { it.first to it.second["status"] })
        databaseReachable = true
        assertEquals(200, check().first)
    }

    @Test
    fun `스키마를 만들지 못해 최신 행 조회도 표가 없어 실패하면 unreachable 이 아니라 failing 이고 사유에 schema 가 남는다`() {
        // 표가 없다는 답은 DB 에 닿았다는 뜻이다 — 그 실패가 스키마 실패라는 진짜 원인을 가리면 안 된다
        tableMissing = true
        sink.schemaFailing = true
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503 to "failing", status to body["status"])
        assertEquals("schema IllegalStateException", body["failure"])
        assertEquals("", body["last_stored_at"])
    }

    @Test
    fun `정상 첫 기동에서 스키마를 만들기 전 표가 없으면 unreachable 이 아니라 starting 이다`() {
        tableMissing = true
        // 첫 바퀴는 그룹을 먼저 만들고 스키마를 만든다 — 그룹은 생겼고 스키마는 아직(DB 가 늦거나 만드는 중)인 때
        stream.ensureGroup()
        val (status, body) = check()
        assertEquals(503 to "starting", status to body["status"])
        assertEquals("", body["failure"])
    }

    @Test
    fun `스키마를 만들 때 DB 에 닿지 못하면 unreachable 이다`() {
        databaseReachable = false
        sink.schemaFailing = true                                       // 연결이 안 되니 스키마도 실패한다
        collector.runOnce()
        val (status, body) = check()
        assertEquals(503 to "unreachable", status to body["status"])
        assertEquals("database IllegalStateException", body["failure"])
    }
}
