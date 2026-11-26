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
 * 삭제는 나눠 지운다: 한 문장에 [retentionBatchSize] 줄까지, 한 문장이 그보다 적게 지우면 끝. 문장이 끝날 때마다 시작부터 흐른 시간이
 * [retentionTimeBudget] 이상이면 멈추고 남긴다(적어도 한 문장은 돈다 — 다음 실행이 잇는다). 문장마다 따로 커밋한다(묶지 않는다) —
 * 수집과 나눠 쓰는 연결을 문장 사이에 수집이 쓸 수 있게.
 */
class VisitSchedule(
    private val collector: VisitCollector,
    private val retention: VisitRetention,
    private val retentionBatchSize: Int = 5_000,
    private val retentionTimeBudget: Duration = Duration.ofMinutes(5),
    private val clock: () -> Instant = Instant::now,                    // 예산을 잴 시계
    /** 뼈대(V2b): 스트림의 안전망 — 기본은 수집기의 지우개 */
    private val cleaner: VisitStreamCleaner? = collector.cleaner,
) {
    /** 뼈대(V2b): 마지막 스트림 정리의 결과 — 뜬 뒤 아직 돌지 않았으면 null */
    val lastStreamTrim: VisitStreamTrimResult? get() = null

    /**
     * 뼈대(V2b): [now] 기준 90일보다 오래된 항목을 스트림에서 잘라 낸다(`cleaner.trimOlderThan(now − 90일)` — DB 삭제와 같은 기준 시각). [deleteOldVisits] 가
     * DB 삭제와 **따로** 부른다(한쪽이 실패해도 다른 쪽은 돈다). 잘라 낸 수, 실패하면 null 과 [lastStreamTrim] 에 종류 이름 · ERROR 줄
     * `visit stream trim failed: <종류>`, 성공하면 INFO 줄 `trimmed <n> visit stream entries older than 90 days`. 지우개가 없으면 null. 본문은 구현 워커가 채운다.
     */
    internal fun trimOldVisitStream(now: Instant): Long? = TODO("뼈대: 구현 워커가 채운다")
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

    /**
     * [now] 기준 [VisitStore.RETENTION] 보다 오래된 방문을 지운다 — 기준 시각의 경계를 테스트가 시계를 정해 확인할 수 있게 나눴다.
     * 지운 수를 돌려주고, 실패하면 null. 묶음은 하나씩 커밋되므로 도중에 실패해도 그때까지 지운 수를 [lastRetention] 과 ERROR 줄에 남긴다.
     */
    internal fun deleteOldVisits(now: Instant): Int? {
        val cutoff = now.minus(VisitStore.RETENTION)
        val startedAt = clock()
        var deleted = 0
        try {
            while (true) {
                val deletedNow = retention.deleteOlderThan(cutoff, retentionBatchSize)
                deleted += deletedNow
                if (deletedNow < retentionBatchSize) break
                if (Duration.between(startedAt, clock()) >= retentionTimeBudget) {
                    log.warn("observe:observe:visit retention stopped at the time budget ({}) after deleting {} visits — older visits remain, the next run continues",
                        retentionTimeBudget, deleted)
                    break
                }
            }
        } catch (error: Exception) {
            // 예외의 글은 싣지 않는다 — 방문 표의 오류 글에는 줄의 값이 실릴 수 있다. 종류 이름만
            log.error("observe:observe:visit retention failed after deleting {} visits: {}", deleted, error.javaClass.simpleName)
            retentionResult = RetentionResult(now, deleted, error.javaClass.simpleName)
            return null
        }
        log.info("observe:observe:deleted {} visits older than {} days", deleted, VisitStore.RETENTION.toDays())
        retentionResult = RetentionResult(now, deleted, null)
        return deleted
    }

    private companion object {
        const val FAILURE_PAUSE_MS = 5_000L
        const val IDLE_PAUSE_MS = 50L
    }
}
