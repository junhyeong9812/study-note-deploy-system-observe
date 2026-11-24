package xyz.junproject.observe.metric

import java.time.Duration

/** 시계가 뒤처진 서버(§15.1) — 가장 최근 항목의 수신 시각 − 측정 시각이 [LIMIT] 을 넘으면(같으면 아니다) 그 서버 */
object ClockBehind {
    val LIMIT: Duration = Duration.ofMinutes(1)

    /** [latest] 는 서버마다 측정 시각이 가장 늦은 항목 — 이름순으로 돌려준다 */
    fun servers(latest: List<ResourceSample>): List<String> =
        latest.filter { Duration.between(it.measuredAt, it.receivedAt) > LIMIT }.map { it.server }.distinct().sorted()
}
