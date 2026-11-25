package xyz.junproject.observe.metric

import java.time.Instant

/** 임계값 판정이 읽는 면(resource-metrics task 04) — [MetricStore] 가 구현한다 */
interface MetricSampleSource {
    /** 측정 시각이 `(from, to]` 인 항목 전부(서버 섞임, 컨테이너 · GPU 포함) — 서버 · 측정 시각순 */
    fun samplesMeasuredBetween(from: Instant, to: Instant): List<ResourceSample>

    /** 보관 중인 원본에서 본 서버별 마지막 수신 시각(기간 제한 없음 — 하루 넘게 끊긴 서버도 든다) */
    fun lastReceivedOfEveryServer(): Map<String, Instant>
}
