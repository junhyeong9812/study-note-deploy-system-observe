package xyz.junproject.observe.visit

import xyz.junproject.observe.metric.MetricStreamSource
import java.time.Duration
import java.time.Instant

/** 수집 쪽이 쓰는 면 */
interface VisitSink {
    /** 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema()
    /** 묶음을 저장한다. 이미 있는 항목(같은 스트림 ID)은 건너뛴다. 새로 들어간 줄 수를 돌려준다. 실패는 예외 */
    fun insertAll(visits: List<Visit>): Int
}

/** 방문 스트림(`visits`)의 수집 — 한 바퀴 */
class VisitCollector(
    private val source: MetricStreamSource,
    private val sink: VisitSink,
    private val batchSize: Int = 200,
    private val block: Duration = Duration.ofSeconds(2),
    private val recheckUnacknowledgedEvery: Duration = Duration.ofMinutes(1),
    private val clock: () -> Instant = Instant::now,
) {
    @Volatile var lastLoopAt: Instant? = null; private set
    @Volatile var lastFailure: String? = null; private set
    @Volatile var storedTotal: Long = 0; private set
    @Volatile var queryStripped: Long = 0; private set

    fun droppedCounts(): Map<VisitDropReason, Long> = TODO("visit-tracking V2 구현")

    fun runOnce(): Int = TODO("visit-tracking V2 구현")
}
