package xyz.junproject.observe

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.RetentionResult
import java.time.Duration
import java.time.Instant

/** 도는 것들 — 수집 루프(전용 스레드), 하루 한 번 30일 지난 로그 삭제 */
@Component
class ObserveSchedule(private val collector: LogCollector, private val store: LogStore) {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var running = true
    private lateinit var thread: Thread

    /** 마지막 30일 삭제의 결과(헬스 본문용) — 뜬 뒤 아직 한 번도 돌지 않았으면 null. 삭제가 조용히 실패해 쌓이는 것을 보이게 한다. */
    val lastRetention: RetentionResult? get() = retention
    @Volatile private var retention: RetentionResult? = null      // 스프링이 이 클래스를 open 으로 만들어 private set 을 쓸 수 없다

    @PostConstruct
    fun start() {
        thread = Thread({
            while (running) {
                try {
                    val handled = collector.runOnce()
                    if (collector.lastFailure != null) Thread.sleep(FAILURE_PAUSE_MS)   // 실패가 이어질 때 헛돌지 않게
                    else if (handled == 0) Thread.sleep(IDLE_PAUSE_MS)                  // 읽기가 이미 2초를 기다렸다 — 숨만 고른다
                } catch (_: InterruptedException) {
                    break
                } catch (error: Exception) {
                    log.error("observe:observe:collector loop error — will retry: {}: {}", error.javaClass.simpleName, error.message?.take(200))
                    try { Thread.sleep(FAILURE_PAUSE_MS) } catch (_: InterruptedException) { break }
                }
            }
        }, "observe-collector").apply { isDaemon = true; start() }
    }

    @PreDestroy
    fun stop() {
        running = false
        thread.interrupt()
        thread.join(5_000)
    }

    @Scheduled(cron = "\${observe.retention-cron:0 40 3 * * *}", zone = "Asia/Seoul")
    fun deleteOldLogs() { deleteOldLogs(Instant.now()) }

    /** [now] 기준 [RETENTION] 보다 오래된 로그를 지운다 — 기준 시각의 경계를 테스트가 시계를 정해 확인할 수 있게 나눴다 */
    internal fun deleteOldLogs(now: Instant): Int? =
        runCatching { store.deleteOlderThan(now.minus(RETENTION)) }
            .onSuccess {
                log.info("observe:observe:deleted {} logs older than {} days", it, RETENTION.toDays())
                retention = RetentionResult(now, it, null)
            }
            .onFailure {
                log.error("observe:observe:retention failed: {}: {}", it.javaClass.simpleName, it.message?.take(200))
                retention = RetentionResult(now, null, it.javaClass.simpleName)   // 문구는 위 로그에만 — 헬스는 인증이 없다
            }
            .getOrNull()

    companion object {
        val RETENTION: Duration = Duration.ofDays(30)
        private const val FAILURE_PAUSE_MS = 5_000L
        private const val IDLE_PAUSE_MS = 50L
    }
}
