package xyz.junproject.observe.collect

import java.time.Duration
import java.time.Instant

/**
 * 수집이 살아 있는가의 판정.
 * - DB·Redis 에 지금 닿는지를 헬스를 물을 때마다 직접 본다 — 스트림이 조용하면 수집 바퀴가 DB 에 닿지 않으므로
 *   바퀴 결과만으로는 "DB 가 죽었는데 정상"으로 보인다. 닿지 못하면 503(unreachable).
 * - 수집 바퀴가 [stallAfter] 안에 한 번이라도 끝까지 돌았고(읽을 것이 없었어도) 마지막 바퀴가 실패하지 않았으면 정상
 * - 뜬 지 [stallAfter] 가 안 됐고 아직 첫 바퀴 전이면 starting(200)
 * - 그 밖은 503 — 바퀴가 실패하고 있거나(failing), 바퀴가 멈췄다(stalled)
 */
class CollectorHealth(
    private val collector: LogCollector,
    private val unacknowledged: () -> Long,           // 받았지만 확인하지 못한 건수 — 물어볼 수 없으면 예외(= Redis 에 닿지 못함)
    private val databaseCheck: () -> Unit,            // DB 에 닿는가 — 닿지 못하면 예외
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
) {
    data class Report(val healthy: Boolean, val body: Map<String, Any>)

    private fun reason(error: Throwable) = "${error.javaClass.simpleName}: ${error.message?.take(200)}"

    fun report(now: Instant): Report {
        val lastLoopAt = collector.lastLoopAt
        val failure = collector.lastFailure
        val pending = runCatching(unacknowledged)
        val database = runCatching(databaseCheck)
        val unreachable = listOfNotNull(
            pending.exceptionOrNull()?.let { "redis ${reason(it)}" },
            database.exceptionOrNull()?.let { "database ${reason(it)}" },
        )
        val status = when {
            unreachable.isNotEmpty() -> "unreachable"
            failure != null -> "failing"
            lastLoopAt != null && Duration.between(lastLoopAt, now) < stallAfter -> "ok"
            lastLoopAt == null && Duration.between(startedAt, now) < stallAfter -> "starting"
            else -> "stalled"
        }
        val body = mapOf(
            "status" to status,
            "last_loop_at" to (lastLoopAt?.toString() ?: ""),
            "last_stored_at" to (collector.lastStoredAt?.toString() ?: ""),   // 마지막으로 저장한 로그가 찍힌 시각
            "stored_since_start" to collector.storedTotal,
            "unacknowledged" to (pending.getOrNull() ?: -1L),
            "failure" to (unreachable.joinToString("; ").ifEmpty { failure ?: "" }),
        )
        return Report(status == "ok" || status == "starting", body)
    }
}
