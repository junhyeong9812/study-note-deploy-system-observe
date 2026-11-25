package xyz.junproject.observe.traffic

import org.slf4j.LoggerFactory
import xyz.junproject.observe.collect.HealthReads
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
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var lastLoggedProblem: String? = null

    /** `traffic` 객체 — 예외를 내지 않는다 */
    fun report(now: Instant): Map<String, Any> {
        val lastLoopAt = collector.lastLoopAt
        val failure = collector.lastFailure
        val deadline = System.nanoTime() + readBudget.toNanos()
        val pendingRead = HealthReads.start(unacknowledged)
        val storedRead = HealthReads.start(latestStoredAt)
        val lagRead = HealthReads.start(lag)
        val progressRead = HealthReads.start(serverProgress)
        val pending = HealthReads.await(pendingRead, deadline)
        val stored = HealthReads.await(storedRead, deadline).recoverCatching { if (HealthReads.isTableMissing(it)) null else throw it }
        val problems = listOfNotNull(pending.exceptionOrNull()?.let { "redis" to it }, stored.exceptionOrNull()?.let { "database" to it })
        val detail = problems.joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}" }.ifEmpty { null }
        if (detail != null && detail != lastLoggedProblem) log.warn("observe:observe:traffic health check unreachable: {}", detail)
        lastLoggedProblem = detail
        val status = when {
            problems.isNotEmpty() -> "unreachable"
            failure != null -> "failing"
            lastLoopAt != null && Duration.between(lastLoopAt, now) < stallAfter -> "ok"
            lastLoopAt == null && Duration.between(startedAt, now) < stallAfter -> "starting"
            else -> "stalled"
        }
        val dropped = collector.droppedCounts()
        val cellDrops = collector.cellDropCounts()
        val rollup = runCatching(lastRollup).getOrNull()
        val retention = runCatching(lastRetention).getOrNull()
        val servers = HealthReads.await(progressRead, deadline).getOrNull().orEmpty().map {
            mapOf(
                "server" to it.server,
                "read_progress_at" to (it.readProgressAt?.toString() ?: ""),
                "receive_delayed" to TrafficReadProgress.isReceiveDelayed(it.progressAdvancedAt, now),
                "sequence_gaps" to it.sequenceGaps,
                "last_received_at" to (it.lastReceivedAt?.toString() ?: ""),
                "first_seq_seen" to (it.firstSeqSeen ?: -1L),
            )
        }
        return mapOf(
            "status" to status,
            "last_loop_at" to (lastLoopAt?.toString() ?: ""),
            "last_stored_at" to (stored.getOrNull()?.toString() ?: ""),
            "stored_since_start" to collector.storedTotal,
            "unacknowledged" to (pending.getOrNull() ?: -1L),
            "lag" to (HealthReads.await(lagRead, deadline).getOrNull() ?: -1L),
            "failure" to (detail ?: failure ?: ""),
            "dropped" to TrafficDropReason.entries.associate { it.wireName to (dropped[it] ?: 0L) },
            "dropped_total" to dropped.values.sum(),
            "skipped_other_kind" to collector.skippedOtherKind,
            "duplicates" to collector.duplicates,
            "cells_dropped" to TrafficCellDrop.entries.associate { it.wireName to (cellDrops[it] ?: 0L) },
            "at_ahead" to collector.atAhead,
            "at_too_old" to collector.atTooOld,
            "requests_rejected" to collector.requestsRejected,
            "last_store_failure" to (collector.lastStoreFailure ?: ""),
            "last_store_failure_at" to (collector.lastStoreFailureAt?.toString() ?: ""),
            "servers" to servers,
            "last_rollup_at" to (runCatching(lastRollupSuccessAt).getOrNull()?.toString() ?: ""),
            "last_rollup_failure" to (rollup?.failure ?: ""),
            "last_rollup_failed_buckets" to (rollup?.failedBuckets ?: -1),
            "last_retention_at" to (retention?.ranAt?.toString() ?: ""),
            "last_retention_entries_deleted" to (retention?.deletion?.entriesDeleted ?: -1),
            "last_retention_segments_deleted" to (retention?.deletion?.segmentsDeleted ?: -1),
            "last_retention_requests_deleted" to (retention?.deletion?.requestsDeleted ?: -1),
            "last_retention_rollup_deleted" to (retention?.deletion?.rollupDeleted ?: -1),
            "last_retention_failure" to (retention?.failure ?: ""),
        )
    }
}
