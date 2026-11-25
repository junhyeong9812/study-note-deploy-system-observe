package xyz.junproject.observe.metric

import java.time.Duration
import java.time.Instant

/** 값 하나의 묶음 — 평균 · 최댓값 · 표본 수(null 값은 표본에 들지 않는다) */
data class MetricAggregate(val average: Double, val maximum: Double, val samples: Long) {
    companion object {
        /**
         * 더 큰 간격으로 묶기(§11.3) — 표본 수로 가중한 평균과 최댓값의 최댓값, 표본 수의 합.
         * 표본이 0 인 조각은 빼고, 남는 것이 없으면 null.
         */
        fun combine(parts: List<MetricAggregate>): MetricAggregate? {
            val counted = parts.filter { it.samples > 0 }
            if (counted.isEmpty()) return null
            val samples = counted.sumOf { it.samples }
            return MetricAggregate(counted.sumOf { it.average * it.samples } / samples, counted.maxOf { it.maximum }, samples)
        }
    }
}

/** 조회 응답의 `source` 글자 */
enum class SeriesSource(val wireName: String) { RAW("raw"), ROLLUP("rollup"), MIXED("mixed") }

/**
 * `GET /metrics/series` 의 간격과 읽는 곳(§11.6).
 * - [step]: 기간을 [MAX_POINTS] 로 나눈 값을 [STEP_UNIT] 단위로 올린 것(최소 [STEP_UNIT]). 5분 평균을 읽는 부분이 있으면 최소 [ROLLUP_STEP].
 * - [rawSince]: `now − 7일` 을 5분 단위로 내린 시각. 측정 시각이 이보다 앞선 부분은 5분 평균에서, 이후는 원본에서 읽는다
 *   (5분 경계에 맞춰야 한 5분 구간이 원본과 평균에서 두 번 읽히지 않는다).
 * - [source]: `from ≥ rawSince` 면 raw, `to ≤ rawSince` 면 rollup, 그 밖은 mixed.
 * 점은 `from` 에서 시작하는 [step] 칸이고 기간은 `[from, to)` — 점은 `ceil((to − from) / step)` ≤ 500 개를 넘지 않는다.
 */
data class SeriesPlan(val step: Duration, val source: SeriesSource, val rawSince: Instant) {
    companion object {
        const val MAX_POINTS = 500
        val STEP_UNIT: Duration = Duration.ofSeconds(30)
        val ROLLUP_STEP: Duration = Duration.ofMinutes(5)
        val RAW_KEPT: Duration = Duration.ofDays(7)

        fun choose(from: Instant, to: Instant, now: Instant): SeriesPlan {
            val rawSince = floorTo(now.minus(RAW_KEPT), ROLLUP_STEP)
            val source = when {
                !from.isBefore(rawSince) -> SeriesSource.RAW
                !to.isAfter(rawSince) -> SeriesSource.ROLLUP
                else -> SeriesSource.MIXED
            }
            val unit = STEP_UNIT.toMillis()
            val perPoint = Math.ceilDiv(Duration.between(from, to).toMillis().coerceAtLeast(0), MAX_POINTS.toLong())
            var step = Math.ceilDiv(perPoint, unit).coerceAtLeast(1) * unit
            if (source != SeriesSource.RAW) step = step.coerceAtLeast(ROLLUP_STEP.toMillis())
            return SeriesPlan(Duration.ofMillis(step), source, rawSince)
        }

        /** [at] 을 [unit] 단위(UTC epoch 기준)로 내린 시각 */
        fun floorTo(at: Instant, unit: Duration): Instant = Instant.ofEpochMilli(Math.floorDiv(at.toEpochMilli(), unit.toMillis()) * unit.toMillis())
    }
}
