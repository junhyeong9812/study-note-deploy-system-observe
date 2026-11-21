package xyz.junproject.observe.collect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/** 명세 ②: /health — 마지막으로 저장한 로그의 시각, 아직 확인하지 못한 건수. DB 나 Redis 에 닿지 못하면 503. */
class CollectorHealthTest {
    private val stream = FakeLogStream()
    private val sink = FakeLogSink()
    private val startedAt = Instant.parse("2026-11-21T00:00:00Z")
    private var now = startedAt
    private var databaseReachable = true
    private val collector = LogCollector(stream, sink, block = Duration.ZERO, clock = { now })
    private val health = CollectorHealth(
        collector,
        unacknowledged = { stream.unacknowledgedCount() },
        databaseCheck = { if (!databaseReachable) throw IllegalStateException("DB 에 닿지 못함") },
        startedAt = startedAt,
    )

    private fun check() = health.report(now).let { (if (it.healthy) 200 else 503) to it.body }

    @Test
    fun `방금 한 바퀴를 돌았으면 200 이고 마지막 저장 시각과 미확인 건수를 보인다`() {
        stream.add(StreamRecord("1763700000123-0", "info", "req-1:backend:x"))
        collector.runOnce()
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
    fun `첫 바퀴 전에는 starting 이지만 1분이 지나도 못 돌면 503`() {
        stream.ensureGroup()
        assertEquals(200 to "starting", check().let { it.first to it.second["status"] })
        now = startedAt.plusSeconds(120)
        assertEquals(503, check().first)
    }

    @Test
    fun `마지막 바퀴가 1분 넘게 전이면 멈춘 것으로 보고 503`() {
        collector.runOnce()
        now = now.plusSeconds(120)
        assertEquals(503 to "stalled", check().let { it.first to it.second["status"] })
    }
}
