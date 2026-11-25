package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * 명세 §11.6 — `step_ms` = 기간 ÷ 500 을 30초 단위로 올린 것(최소 30초), 7일보다 오래된 구간이 있으면 최소 5분,
 * `source` raw·rollup·mixed, 점은 500 개를 넘지 않는다. §11.3 — 큰 간격은 표본 수 가중 평균과 최댓값의 최댓값.
 */
class SeriesPlanTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")          // 5분 경계에 맞춘 지금
    private val sevenDaysAgo = now.minus(Duration.ofDays(7))

    private fun plan(span: Duration, to: Instant = now) = SeriesPlan.choose(to.minus(span), to, now)
    private fun pointsOf(span: Duration, step: Duration) = (span.toMillis() + step.toMillis() - 1) / step.toMillis()

    @Test
    fun `점 499·500·501 개에 해당하는 기간 — 500 개까지는 30초, 넘으면 60초로 올린다`() {
        assertEquals(Duration.ofSeconds(30), plan(Duration.ofSeconds(499L * 30)).step)
        assertEquals(Duration.ofSeconds(30), plan(Duration.ofSeconds(500L * 30)).step)
        assertEquals(500L, pointsOf(Duration.ofSeconds(500L * 30), Duration.ofSeconds(30)))
        assertEquals(Duration.ofSeconds(60), plan(Duration.ofSeconds(501L * 30)).step)
        assertEquals(Duration.ofSeconds(60), plan(Duration.ofSeconds(500L * 30).plusMillis(1)).step, "1ms 만 넘어도 올린다")
    }

    @Test
    fun `짧은 기간도 최소 30초이고 31일은 5370초`() {
        assertEquals(Duration.ofSeconds(30), plan(Duration.ofHours(1)).step)
        assertEquals(Duration.ofSeconds(30), plan(Duration.ZERO).step)
        assertEquals(Duration.ofSeconds(30), plan(Duration.ofMillis(1)).step)
        // 31일 = 2,678,400초 ÷ 500 = 5,356.8초 → 30초 단위로 올려 5,370초
        assertEquals(Duration.ofSeconds(5_370), plan(Duration.ofDays(31)).step)
    }

    @Test
    fun `7일 안이면 raw, 7일 경계를 걸치면 mixed 이고 최소 5분, 경계 앞만이면 rollup`() {
        val exactlySeven = SeriesPlan.choose(sevenDaysAgo, now, now)
        assertEquals(SeriesSource.RAW, exactlySeven.source)
        assertEquals(sevenDaysAgo, exactlySeven.rawSince)
        assertEquals(Duration.ofSeconds(1_230), exactlySeven.step)   // 604,800 ÷ 500 = 1,209.6 → 1,230

        val crossing = SeriesPlan.choose(sevenDaysAgo.minusMillis(1), now, now)
        assertEquals(SeriesSource.MIXED, crossing.source)

        // 짧은 기간이 경계를 걸치면 30초가 아니라 5분
        val shortCrossing = SeriesPlan.choose(sevenDaysAgo.minus(Duration.ofMinutes(1)), sevenDaysAgo.plus(Duration.ofMinutes(1)), now)
        assertEquals(SeriesSource.MIXED to Duration.ofMinutes(5), shortCrossing.source to shortCrossing.step)

        val oldOnly = SeriesPlan.choose(now.minus(Duration.ofDays(8)), sevenDaysAgo, now)
        assertEquals(SeriesSource.ROLLUP to Duration.ofMinutes(5), oldOnly.source to oldOnly.step, "하루 ÷ 500 = 172.8초 → 180초지만 5분 평균이라 5분")

        val oldShort = SeriesPlan.choose(now.minus(Duration.ofDays(20)), now.minus(Duration.ofDays(20)).plus(Duration.ofHours(1)), now)
        assertEquals(SeriesSource.ROLLUP to Duration.ofMinutes(5), oldShort.source to oldShort.step)

        val oldLong = SeriesPlan.choose(now.minus(Duration.ofDays(30)), now.minus(Duration.ofDays(10)), now)
        assertEquals(SeriesSource.ROLLUP to Duration.ofSeconds(3_480), oldLong.source to oldLong.step)   // 20일 ÷ 500 = 3,456 → 3,480
    }

    @Test
    fun `원본과 평균의 경계는 지금 − 7일 을 5분 단위로 내린 시각이다`() {
        // 5분 구간 하나가 원본과 평균에서 두 번 읽히지 않게
        val unaligned = Instant.parse("2026-11-24T03:02:10Z")
        val plan = SeriesPlan.choose(unaligned.minus(Duration.ofHours(1)), unaligned, unaligned)
        assertEquals(Instant.parse("2026-11-17T03:00:00Z"), plan.rawSince)
        assertEquals(SeriesSource.RAW, plan.source)
        assertEquals(SeriesSource.MIXED, SeriesPlan.choose(Instant.parse("2026-11-17T02:59:59Z"), unaligned, unaligned).source)
        assertEquals(SeriesSource.RAW, SeriesPlan.choose(Instant.parse("2026-11-17T03:00:00Z"), unaligned, unaligned).source)
    }

    @Test
    fun `어떤 기간이든 점은 500 개를 넘지 않고 간격은 30초의 배수다`() {
        val spans = (0..2_000).map { Duration.ofSeconds(it * 1_337L + it % 7) } + Duration.ofDays(31) + Duration.ofDays(7).plusMillis(1)
        for (span in spans.filter { it <= Duration.ofDays(31) }) {
            for (to in listOf(now, now.minus(Duration.ofDays(3)), now.minus(Duration.ofDays(9)))) {
                val plan = SeriesPlan.choose(to.minus(span), to, now)
                assertTrue(pointsOf(span, plan.step) <= SeriesPlan.MAX_POINTS, "$span → ${plan.step}")
                assertEquals(0L, plan.step.toMillis() % 30_000, "$span → ${plan.step}")
                assertTrue(plan.step >= Duration.ofSeconds(30))
                if (plan.source != SeriesSource.RAW) assertTrue(plan.step >= Duration.ofMinutes(5), "$span ${plan.source} → ${plan.step}")
            }
        }
    }
}

/** 명세 §11.3 — 더 큰 간격으로 묶을 때 표본 수 가중 평균, 최댓값의 최댓값 */
class MetricAggregateTest {
    @Test
    fun `평균의 단순 평균이 아니라 표본 수로 가중한다`() {
        val combined = MetricAggregate.combine(listOf(MetricAggregate(10.0, 12.0, 1), MetricAggregate(40.0, 50.0, 3)))
        assertEquals(MetricAggregate(32.5, 50.0, 4), combined)
    }

    @Test
    fun `표본이 0 인 조각은 빠지고 남는 것이 없으면 null`() {
        assertNull(MetricAggregate.combine(emptyList()))
        assertNull(MetricAggregate.combine(listOf(MetricAggregate(0.0, 0.0, 0))))
        assertEquals(MetricAggregate(7.0, 9.0, 2), MetricAggregate.combine(listOf(MetricAggregate(0.0, 0.0, 0), MetricAggregate(7.0, 9.0, 2))))
    }

    @Test
    fun `조각 하나면 그대로`() {
        assertEquals(MetricAggregate(3.0, 5.0, 10), MetricAggregate.combine(listOf(MetricAggregate(3.0, 5.0, 10))))
    }
}
