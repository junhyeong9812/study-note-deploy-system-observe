package xyz.junproject.observe.visit

import xyz.junproject.observe.log.RetentionResult
import java.time.Duration
import java.time.Instant

/** 방문 수집의 헬스 칸 — `/health` 본문의 `visits` 객체. 로그 수집의 `status` 와 HTTP 상태에는 넣지 않는다 */
class VisitHealth(
    private val collector: VisitCollector,
    private val unacknowledged: () -> Long,
    private val lag: () -> Long?,
    private val latestStoredAt: () -> Instant?,
    private val lastRetention: () -> RetentionResult?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
    private val readBudget: Duration = Duration.ofSeconds(2),
) {
    fun report(now: Instant): Map<String, Any> = TODO("visit-tracking V2 구현")
}
