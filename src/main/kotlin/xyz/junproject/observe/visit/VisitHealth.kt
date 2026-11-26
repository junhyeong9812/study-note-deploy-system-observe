package xyz.junproject.observe.visit

import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.RetentionResult
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * 방문 수집의 헬스 칸 — `/health` 본문의 `visits` 객체. 로그 수집의 `status` 와 HTTP 상태에는 넣지 않는다.
 * 실패 사유는 예외 종류 이름만 — 헬스는 인증이 없고, 방문 쪽 예외의 글에는 주소가 실릴 수 있어 서버 로그에도 종류 이름만 남긴다.
 *
 * 칸(값이 없으면 빈 문자열 · -1 — 지표 칸과 같은 관례):
 * - `status`: 지표 칸과 같은 낱말과 순서 — `unreachable`(Redis 의 미확인 건수나 DB 의 마지막 저장 시각을 묻지 못함, 표 없음 42P01 은 닿은 것) →
 *   `failing`(마지막 바퀴 실패) → `ok`(마지막 성공 바퀴가 [stallAfter] 안) → `starting`(뜬 지 [stallAfter] 안, 아직 성공 없음) → `stalled`.
 *   `ok` 일 때 가장 오래된 줄이 `now − (90일 + [RETENTION_GRACE])` 보다 엄격히 오래되면 `retention_overdue`(90일 삭제가 밀림 — `ok` 만 대신한다).
 * - `last_loop_at` · `last_stored_at`(가장 최근 방문의 스트림 시각) · `oldest_stored_at`(가장 오래된 방문의 스트림 시각) · `stored_since_start` ·
 *   `already_stored`(받았지만 표에 이미 있던 항목 수) · `store_result_unknown`(저장소가 "결과 모름"을 돌려준 항목 수) · `unacknowledged` · `lag` · `failure`
 * - `dropped`: 이유 글자 → 건수(모든 이유, 없으면 0) · `dropped_total` · `query_stripped`
 * - `last_retention_at` · `last_retention_deleted` · `last_retention_failure`(90일 삭제)
 * - 스트림에서 지우기: `stream_deleted` · `stream_delete_failed` · `stream_delete_pending` · `stream_delete_forgotten`(수집기가 센 수), 하루 한 번의 정리: `last_stream_trim_at` ·
 *   `last_stream_trimmed` · `last_stream_trim_failure` — 상태 판정에는 넣지 않는다(지우기의 실패는 수집의 실패가 아니다)
 * 바깥에 묻는 것(미확인 · 밀린 건수, 마지막 · 가장 오래된 저장 시각)은 한꺼번에 시작해 합쳐 [readBudget] 안에서만 기다린다 — 넘기면 `TimeoutException` 실패와 같다.
 */
class VisitHealth(
    private val collector: VisitCollector,
    private val unacknowledged: () -> Long,
    private val lag: () -> Long?,
    private val latestStoredAt: () -> Instant?,
    private val lastRetention: () -> RetentionResult?,
    private val oldestStoredAt: () -> Instant?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
    private val readBudget: Duration = Duration.ofSeconds(2),
    /** 마지막 스트림 정리(하루 한 번의 안전망) — 칸 `last_stream_trim_*` */
    private val lastStreamTrim: () -> VisitStreamTrimResult? = { null },
) {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var lastLoggedProblem: String? = null

    /** `visits` 객체 — 예외를 내지 않는다(묻지 못한 것은 위 규칙대로 칸에 남긴다) */
    fun report(now: Instant): Map<String, Any> {
        val lastLoopAt = collector.lastLoopAt
        val failure = collector.lastFailure
        val deadline = System.nanoTime() + readBudget.toNanos()
        val pendingRead = startRead(unacknowledged)
        val storedRead = startRead(latestStoredAt)
        val lagRead = startRead(lag)
        val oldestRead = startRead(oldestStoredAt)
        val pending = awaitRead(pendingRead, deadline)
        val stored = awaitRead(storedRead, deadline).recoverCatching { if (isTableMissing(it)) null else throw it }
        val oldest = awaitRead(oldestRead, deadline).recoverCatching { if (isTableMissing(it)) null else throw it }
        val databaseError = stored.exceptionOrNull() ?: oldest.exceptionOrNull()     // 같은 DB — 사유는 하나만
        val problems = listOfNotNull(pending.exceptionOrNull()?.let { "redis" to it }, databaseError?.let { "database" to it })
            .joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}" }
        if (problems != lastLoggedProblem && problems.isNotEmpty()) log.warn("observe:observe:visit health check unreachable: {}", problems)
        lastLoggedProblem = problems
        val status = when {
            problems.isNotEmpty() -> "unreachable"
            failure != null -> "failing"
            lastLoopAt != null && Duration.between(lastLoopAt, now) < stallAfter ->
                if (oldest.getOrNull()?.isBefore(now.minus(VisitStore.RETENTION).minus(RETENTION_GRACE)) == true) "retention_overdue" else "ok"
            lastLoopAt == null && Duration.between(startedAt, now) < stallAfter -> "starting"
            else -> "stalled"
        }
        val dropped = collector.droppedCounts()
        val retention = runCatching(lastRetention).getOrNull()
        val streamTrim = runCatching(lastStreamTrim).getOrNull()
        return mapOf(
            "status" to status,
            "last_loop_at" to (lastLoopAt?.toString() ?: ""),
            "last_stored_at" to (stored.getOrNull()?.toString() ?: ""),
            "oldest_stored_at" to (oldest.getOrNull()?.toString() ?: ""),
            "stored_since_start" to collector.storedTotal,
            "already_stored" to collector.alreadyStored,
            "store_result_unknown" to collector.storeResultUnknown,
            "unacknowledged" to (pending.getOrNull() ?: -1L),
            "lag" to (awaitRead(lagRead, deadline).getOrNull() ?: -1L),
            "failure" to problems.ifEmpty { failure ?: "" },
            "dropped" to VisitDropReason.entries.associate { it.wireName to (dropped[it] ?: 0L) },
            "dropped_total" to dropped.values.sum(),
            "query_stripped" to collector.queryStripped,
            "last_retention_at" to (retention?.ranAt?.toString() ?: ""),
            "last_retention_deleted" to (retention?.deleted ?: -1),
            "last_retention_failure" to (retention?.failure ?: ""),
            "stream_deleted" to collector.streamDeleted,
            "stream_delete_failed" to collector.streamDeleteFailed,
            "stream_delete_pending" to collector.streamDeletePending.toLong(),
            "stream_delete_forgotten" to collector.streamDeleteForgotten,
            "last_stream_trim_at" to (streamTrim?.ranAt?.toString() ?: ""),
            "last_stream_trimmed" to (streamTrim?.trimmed ?: -1L),
            "last_stream_trim_failure" to (streamTrim?.failure ?: ""),
        )
    }

    /** 읽기 하나를 가상 스레드에서 시작한다 */
    private fun <T> startRead(read: () -> T): CompletableFuture<T> = CompletableFuture.supplyAsync({ read() }, READERS)

    /** 마감까지 기다린 결과 — 마감을 넘기면 [TimeoutException] 실패(늦은 읽기는 버려 두고 기다리지 않는다). 읽기의 예외는 그대로 */
    private fun <T> awaitRead(read: CompletableFuture<T>, deadline: Long): Result<T> = try {
        Result.success(read.get(maxOf(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
    } catch (error: ExecutionException) {
        Result.failure(error.cause ?: error)
    } catch (error: TimeoutException) {
        read.cancel(true)
        Result.failure(error)
    } catch (error: InterruptedException) {
        Thread.currentThread().interrupt()
        Result.failure(error)
    }

    private fun isTableMissing(error: Throwable) =
        generateSequence(error) { it.cause }.any { it is SQLException && it.sqlState == UNDEFINED_TABLE }

    private companion object {
        val READERS = Executors.newVirtualThreadPerTaskExecutor()!!
        const val UNDEFINED_TABLE = "42P01"   // PostgreSQL: relation does not exist
        /** 하루 한 번의 삭제가 한 번 늦거나 예산에 걸려 남겨도 밀렸다고 보지 않는 여유(하루 + 2시간) */
        val RETENTION_GRACE: Duration = Duration.ofHours(26)
    }
}
