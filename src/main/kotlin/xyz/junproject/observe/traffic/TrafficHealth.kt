package xyz.junproject.observe.traffic

import java.time.Duration
import java.time.Instant

/**
 * 통신 수집의 헬스 칸 — `/health` 본문의 `traffic` 객체. **로그 수집의 `status` 와 HTTP 상태에는 넣지 않는다**
 * ([xyz.junproject.observe.collect.CollectorHealth] 가 판정이 끝난 뒤에 덧붙이고, 만들다 실패해도 이 칸에만 남긴다).
 * 실패 사유는 예외 종류 이름만(헬스는 인증이 없다). 바깥에 묻는 것은 한꺼번에 시작해 합쳐 [readBudget] 안에서만 기다린다(지표 칸과 같은 방식).
 *
 * 칸(값이 없으면 빈 문자열 · -1 — 기존 칸과 같은 관례):
 * - `status`(`unreachable` → `failing` → `ok` → `starting` → `stalled` — 지표 칸과 같은 판정) · `last_loop_at` · `last_stored_at` · `stored_since_start` ·
 *   `unacknowledged` · `lag` · `failure`
 * - `dropped`(이유 글자 → 수, 모든 이유) · `dropped_total` · `skipped_other_kind` · `duplicates`
 * - `cells_dropped`(`segment_ahead` · `segment_too_old` · `request_ahead` · `request_too_old` → 수) · `at_ahead` · `at_too_old` · `requests_rejected`
 * - `last_store_failure` · `last_store_failure_at`
 * - `servers`: `[{server, read_progress_at, receive_delayed, sequence_gaps, last_received_at, first_seq_seen}]`(읽지 못하면 빈 목록 · 없는 값은 "" · -1)
 * - `last_rollup_at` · `last_rollup_failure` · `last_rollup_failed_buckets` · `last_retention_at` · `last_retention_entries_deleted` ·
 *   `last_retention_segments_deleted` · `last_retention_requests_deleted` · `last_retention_rollup_deleted` · `last_retention_failure`
 * 뼈대: 본문은 구현 워커가 채운다.
 */
class TrafficHealth(
    private val collector: TrafficCollector,
    private val unacknowledged: () -> Long,
    private val lag: () -> Long?,
    private val latestStoredAt: () -> Instant?,
    private val serverProgress: () -> List<TrafficServerProgress>,
    private val lastRollup: () -> TrafficRollupResult?,
    private val lastRollupSuccessAt: () -> Instant?,
    private val lastRetention: () -> TrafficRetentionResult?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
    private val readBudget: Duration = Duration.ofSeconds(2),
) {
    /** `traffic` 객체 — 예외를 내지 않는다 */
    fun report(now: Instant): Map<String, Any> = TODO("뼈대: 구현 워커가 채운다")
}
