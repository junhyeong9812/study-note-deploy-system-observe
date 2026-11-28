package xyz.junproject.observe.traffic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 규약 4.2 · 4.5 의 순수 규칙 — 칸끼리 더하는 법, 분포에서 중앙값 · 95% 를 내는 법(든 칸의 위쪽 경계, 보간 없음),
 * 칸마다의 앞섬 2분 · 오래됨 6일의 경계, "채우는 중"(읽기 진도 − 30초 뒤에서 끝나는 칸) · "수신 지연"(2분 넘게 멈춤)의 경계.
 */
class TrafficRulesTest {
    private fun buckets(vararg pairs: Pair<Int, Long>) = LongArray(20).also { array -> pairs.forEach { (index, value) -> array[index] = value } }.toList()

    @Test
    fun `칸을 더하면 수 · 상태 · 분포는 칸마다 더하고 max_ms 는 큰 쪽이다`() {
        val first = TrafficTotals(3, TrafficStatusCounts(2, 0, 1, 0, 0), buckets(0 to 2L, 5 to 1L), 240)
        val second = TrafficTotals(2, TrafficStatusCounts(0, 1, 0, 1, 0), buckets(5 to 1L, 19 to 1L), 190_000)
        assertEquals(TrafficTotals(5, TrafficStatusCounts(2, 1, 1, 1, 0), buckets(0 to 2L, 5 to 2L, 19 to 1L), 190_000), first + second)
        assertEquals(TrafficTotals(5, TrafficStatusCounts(2, 1, 1, 1, 0), buckets(0 to 2L, 5 to 2L, 19 to 1L), 190_000), second + first)
        assertEquals(first, TrafficTotals.EMPTY + first, "빈 합에 더해 max_ms 가 null 로 남았다")
        val health = TrafficTotals(4, TrafficStatusCounts(4, 0, 0, 0, 0), List(20) { 0L }, null)
        assertNull((health + health).maxMs)
    }

    @Test
    fun `분위수는 든 칸의 위쪽 경계이고 마지막 칸이면 180초 초과다`() {
        // 순위 = ceil(합 × 분위 / 100) — 합 20 이면 중앙값은 10 번째, 95% 는 19 번째
        val spread = buckets(0 to 10L, 5 to 8L, 7 to 1L, 19 to 1L)
        assertEquals(TrafficQuantile(5, null), TrafficDistribution.quantile(spread, 50))
        assertEquals(TrafficQuantile(1000, null), TrafficDistribution.quantile(spread, 95))
        assertEquals(TrafficQuantile(null, 180_000), TrafficDistribution.quantile(spread, 100))
        // 합 2: 중앙값은 1 번째(첫 칸), 95% 는 2 번째
        assertEquals(TrafficQuantile(5, null), TrafficDistribution.quantile(buckets(0 to 1L, 1 to 1L), 50))
        assertEquals(TrafficQuantile(10, null), TrafficDistribution.quantile(buckets(0 to 1L, 1 to 1L), 95))
        // 한 칸만 — 그 칸
        assertEquals(TrafficQuantile(180_000, null), TrafficDistribution.quantile(buckets(18 to 3L), 50))
        assertEquals(TrafficQuantile(null, 180_000), TrafficDistribution.quantile(buckets(19 to 3L), 95))
        assertNull(TrafficDistribution.quantile(List(20) { 0L }, 50), "줄이 없는데 분위수를 냈다")
    }

    @Test
    fun `분포의 위쪽 경계는 규약의 19 개 글자 그대로다`() {
        assertEquals(TrafficRecords.UPPER_BOUNDS_MS, TrafficDistribution.UPPER_BOUNDS_MS)
        assertEquals(20, TrafficDistribution.BUCKET_COUNT)
    }

    @Test
    fun `칸의 시각이 2분 넘게 앞서면 앞섬이고 정확히 2분은 아니다`() {
        val receivedAt = Instant.parse("2026-11-25T04:00:00Z")
        assertFalse(TrafficCellAge.isAhead(receivedAt.plus(Duration.ofMinutes(2)), receivedAt))
        assertTrue(TrafficCellAge.isAhead(receivedAt.plus(Duration.ofMinutes(2)).plusMillis(1), receivedAt))
        assertFalse(TrafficCellAge.isTooOld(receivedAt.minus(Duration.ofDays(6)), receivedAt))
        assertTrue(TrafficCellAge.isTooOld(receivedAt.minus(Duration.ofDays(6)).minusMillis(1), receivedAt))
    }

    @Test
    fun `읽기 진도 − 30초 보다 뒤에서 끝나는 칸이 채우는 중이고 같으면 아니다 · 진도가 없으면 모두 채우는 중`() {
        val progress = Instant.parse("2026-11-25T04:01:10Z")
        assertFalse(TrafficReadProgress.isFilling(progress.minusSeconds(30), progress))
        assertTrue(TrafficReadProgress.isFilling(progress.minusSeconds(30).plusMillis(1), progress))
        assertFalse(TrafficReadProgress.isFilling(Instant.parse("2026-11-25T04:00:00Z"), progress))
        assertTrue(TrafficReadProgress.isFilling(Instant.parse("2025-01-01T00:00:00Z"), null))
    }

    @Test
    fun `읽기 진도가 2분 넘게 나아가지 않으면 수신 지연이고 정확히 2분은 아니다`() {
        val advancedAt = Instant.parse("2026-11-25T04:00:00Z")
        assertFalse(TrafficReadProgress.isReceiveDelayed(advancedAt, advancedAt.plus(Duration.ofMinutes(2))))
        assertTrue(TrafficReadProgress.isReceiveDelayed(advancedAt, advancedAt.plus(Duration.ofMinutes(2)).plusMillis(1)))
        assertTrue(TrafficReadProgress.isReceiveDelayed(null, advancedAt), "진도가 없는 서버를 지연 아님으로 보였다")
    }
}
