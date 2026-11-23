package xyz.junproject.observe.metric

import java.time.Duration
import java.time.Instant

/**
 * 지표 수집의 헬스 칸 — `/health` 본문의 `metrics` 객체. **로그 수집의 `status` 와 HTTP 상태에는 넣지 않는다**(§10.3).
 * 실패 사유는 예외 종류 이름만(헬스는 인증이 없다).
 *
 * `metrics` 객체의 칸(값이 없으면 빈 문자열 · -1 — 기존 칸과 같은 관례):
 * - `status`: 로그의 판정과 같은 낱말과 순서 — `unreachable`(지금 Redis 의 미확인 건수나 DB 의 마지막 저장 시각을 묻지 못함,
 *   표 없음 42P01 은 닿은 것으로 본다) → `failing`(마지막 바퀴 실패) → `ok`(마지막 성공 바퀴가 [stallAfter] 안) →
 *   `starting`(뜬 지 [stallAfter] 안, 아직 성공 없음) → `stalled`.
 * - `last_loop_at` · `last_stored_at`(DB 의 가장 최근 항목의 수신 시각) · `stored_since_start` · `unacknowledged` · `lag` · `failure`
 * - `dropped`: 이유 글자([MetricDropReason.wireName]) → 건수(모든 이유, 없으면 0) · `dropped_total` · `skipped_other_kind`
 * - `last_received_at`: 서버 이름 → 마지막 수신 시각(ISO) — 읽지 못하면 빈 객체
 * - `last_rollup_at`(5분 평균의 마지막 **성공** 시각) · `last_rollup_failure`(마지막 바퀴가 실패했으면 그 종류, 성공이면 빈 문자열)
 * - `last_retention_at` · `last_retention_raw_deleted` · `last_retention_rollup_deleted` · `last_retention_failure`
 */
class MetricHealth(
    private val collector: MetricCollector,
    private val unacknowledged: () -> Long,
    private val lag: () -> Long?,
    private val latestStoredAt: () -> Instant?,
    private val lastReceivedByServer: () -> Map<String, Instant>,
    private val lastRollup: () -> MetricRollupResult?,
    private val lastRollupSuccessAt: () -> Instant?,
    private val lastRetention: () -> MetricRetentionResult?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
) {
    /** `metrics` 객체 — 예외를 내지 않는다(묻지 못한 것은 위 규칙대로 칸에 남긴다) */
    fun report(now: Instant): Map<String, Any> = TODO("not implemented")
}
