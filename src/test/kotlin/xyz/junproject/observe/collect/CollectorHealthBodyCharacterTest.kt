package xyz.junproject.observe.collect

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 수집 헬스 본문의 칸 — 지금의 꼴을 고정한다(로그 무수신 판정의 칸을 더해도 기존 칸 · 판정 · HTTP 상태는 그대로):
 * 지표 칸을 붙이지 않은 본문의 칸 열셋 · 한 바퀴를 돈 직후 200 · `ok`.
 */
class CollectorHealthBodyCharacterTest {
    private val now = Instant.parse("2026-11-26T03:00:00Z")
    private val stream = FakeLogStream()
    private val collector = LogCollector(stream, FakeLogSink(), block = Duration.ZERO, clock = { now })

    @Test
    fun `지표 칸을 붙이지 않은 수집 헬스 본문은 지금의 칸 열셋이고 한 바퀴 뒤 200 ok 다`() {
        collector.runOnce()
        val report = CollectorHealth(collector, { stream.unacknowledgedCount() }, { 0L }, { now }, { null }, startedAt = now).report(now)
        assertTrue(report.healthy)
        assertEquals("ok", report.body["status"])
        assertEquals(setOf(
            "status", "last_loop_at", "last_stored_at", "stored_since_start", "unacknowledged", "lag", "failure",
            "last_retention_at", "last_retention_deleted", "last_retention_failure",
            "last_auth_retention_at", "last_auth_retention_deleted", "last_auth_retention_failure",
        ), report.body.keys)
    }
}
