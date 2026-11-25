package xyz.junproject.observe.visit

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import xyz.junproject.observe.log.RetentionResult
import java.time.Duration
import java.time.Instant

/**
 * 방문 수집 스레드와 하루 한 번 90일 삭제.
 * 수집은 로그 · 지표와 **다른 전용 스레드**(`observe-visit-collector`)로 돈다 — 방문 쪽의 어떤 실패도 로그 · 지표 수집을 멈추지 않는다.
 * 예외는 다시 시도하고, Error 는 이 스레드만 끝낸다(그 뒤 헬스의 `visits.status` 가 `stalled`). 로그에는 예외의 종류 이름만 남긴다.
 * 삭제는 기존 삭제와 같은 시각(서울 03:40)에 따로 돈다 — 실패해도 예외를 내지 않고 [lastRetention] 과 로그에 종류 이름만.
 */
class VisitSchedule(
    private val collector: VisitCollector,
    private val retention: VisitRetention,
    private val retentionBatchSize: Int = 5_000,                       // 뼈대: 구현 워커가 한 문장의 묶음 크기로 쓴다
    private val retentionTimeBudget: Duration = Duration.ofMinutes(5),  // 뼈대: 구현 워커가 한 번의 실행의 시간 예산으로 쓴다
    private val clock: () -> Instant = Instant::now,                    // 뼈대: 예산을 잴 시계
) {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var running = true
    private var thread: Thread? = null

    /** 마지막 90일 삭제의 결과(헬스 본문용) — 뜬 뒤 아직 돌지 않았으면 null */
    val lastRetention: RetentionResult? get() = retentionResult
    @Volatile private var retentionResult: RetentionResult? = null

    @PostConstruct
    fun start() {
        thread = Thread({
            try {
                while (running) {
                    try {
                        val handled = collector.runOnce()
                        if (collector.lastFailure != null) Thread.sleep(FAILURE_PAUSE_MS)   // 실패가 이어질 때 헛돌지 않게
                        else if (handled == 0) Thread.sleep(IDLE_PAUSE_MS)                  // 읽기가 이미 2초를 기다렸다 — 숨만 고른다
                    } catch (_: InterruptedException) {
                        break
                    } catch (error: Exception) {
                        log.error("observe:observe:visit collector loop error — will retry: {}", error.javaClass.simpleName)
                        try { Thread.sleep(FAILURE_PAUSE_MS) } catch (_: InterruptedException) { break }
                    }
                }
            } catch (error: Error) {
                log.error("observe:observe:visit collector stopped: {}", error.javaClass.simpleName)
                throw error
            }
        }, "observe-visit-collector").apply { isDaemon = true; start() }
    }

    @PreDestroy
    fun stop() {
        running = false
        thread?.interrupt()
        thread?.join(5_000)
    }

    @Scheduled(cron = "\${observe.visits.retention-cron:0 40 3 * * *}", zone = "Asia/Seoul")
    fun deleteOldVisits() {
        deleteOldVisits(Instant.now())
    }

    /** [now] 기준 [VisitStore.RETENTION] 보다 오래된 방문을 지운다 — 기준 시각의 경계를 테스트가 시계를 정해 확인할 수 있게 나눴다 */
    internal fun deleteOldVisits(now: Instant): Int? =
        runCatching { retention.deleteOlderThan(now.minus(VisitStore.RETENTION), retentionBatchSize) }   // 뼈대: 한 문장뿐 — 나눠 지우기 · 예산은 구현 워커
            .onSuccess {
                log.info("observe:observe:deleted {} visits older than {} days", it, VisitStore.RETENTION.toDays())
                retentionResult = RetentionResult(now, it, null)
            }
            .onFailure {
                log.error("observe:observe:visit retention failed: {}", it.javaClass.simpleName)
                retentionResult = RetentionResult(now, null, it.javaClass.simpleName)
            }
            .getOrNull()

    private companion object {
        const val FAILURE_PAUSE_MS = 5_000L
        const val IDLE_PAUSE_MS = 50L
    }
}
