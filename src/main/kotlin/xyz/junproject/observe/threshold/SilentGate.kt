package xyz.junproject.observe.threshold

import java.time.Duration
import java.time.Instant

/**
 * 끊김 규칙의 시작(§15.1) — 이 프로세스가 뜬 지([startedAt]) [warmUp] 이 지나고(같으면 지난 것) 뜬 뒤로 지표 수집이 한 번이라도 따라잡았으면
 * ([caughtUpAt] 이 null 이 아님) 판정한다. 따라잡지 못했어도 뜬 지 [giveUpAfter] 가 지나면(같으면 지난 것) 판정한다(영영 미루지 않는다).
 * (리뷰 루프 1 — 구현 전)
 */
class SilentGate(
    private val startedAt: Instant,
    private val caughtUpAt: () -> Instant?,
    private val warmUp: Duration = Duration.ofMinutes(3),
    private val giveUpAfter: Duration = Duration.ofMinutes(15),
) {
    fun active(now: Instant): Boolean = TODO("not implemented")
}
