package xyz.junproject.observe

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import xyz.junproject.observe.auth.AuthAttemptPolicy
import xyz.junproject.observe.auth.AuthAttempts
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricDeletion
import xyz.junproject.observe.metric.MetricMaintenance
import xyz.junproject.observe.metric.MetricRetentionResult
import xyz.junproject.observe.metric.MetricRollupResult
import xyz.junproject.observe.metric.MetricStore
import java.time.Duration
import java.time.Instant
import xyz.junproject.observe.threshold.DispatchOutcome
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdRoundResult

/**
 * 도는 것들 — 수집 루프(전용 스레드), 하루 한 번 30일 지난 로그 삭제와 90일 지난 로그인 시도 기록 삭제.
 * [authAttempts] 가 없으면(로그 삭제만 보는 테스트) 시도 기록 삭제를 건너뛴다 — 앱에서는 늘 빈이 들어온다.
 *
 * 지표: [metricCollector] 가 있으면 로그와 **다른 전용 스레드**(`observe-metric-collector`)로
 * 수집 바퀴를 돈다 — 지표 쪽의 어떤 실패도 로그 수집을 멈추지 않는다. [metrics] 가 있으면
 * 5분 평균을 [rollUpMetrics] 가 `@Scheduled(fixedDelayString = "\${observe.metrics.rollup-interval-ms:60000}")` 로 돌고,
 * 7일·30일 삭제는 [deleteOldLogs] 예약 실행에서 로그·로그인 시도 삭제와 **서로 따로** 돈다(한쪽이 실패해도 다른 쪽은 돈다).
 */
@Component
class ObserveSchedule(
    private val collector: LogCollector,
    private val store: LogStore,
    private val authAttempts: AuthAttempts? = null,
    private val authPolicy: AuthAttemptPolicy = AuthAttemptPolicy(),
    private val metricCollector: MetricCollector? = null,
    private val metrics: MetricMaintenance? = null,
    private val thresholdRound: ThresholdRound? = null,
) {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var running = true
    private lateinit var thread: Thread
    private var metricThread: Thread? = null

    /** 마지막 30일 삭제의 결과(헬스 본문용) — 뜬 뒤 아직 한 번도 돌지 않았으면 null. 삭제가 조용히 실패해 쌓이는 것을 보이게 한다. */
    val lastRetention: RetentionResult? get() = retention
    @Volatile private var retention: RetentionResult? = null      // 스프링이 이 클래스를 open 으로 만들어 private set 을 쓸 수 없다

    /** 마지막 로그인 시도 기록 삭제의 결과(헬스 본문용) — 뜬 뒤 아직 돌지 않았으면 null */
    val lastAuthRetention: RetentionResult? get() = authRetention
    @Volatile private var authRetention: RetentionResult? = null

    /** 마지막 지표 삭제(원본 7일 · 5분 평균 30일)의 결과(헬스 본문용) — 뜬 뒤 아직 돌지 않았으면 null */
    val lastMetricRetention: MetricRetentionResult? get() = metricRetention
    @Volatile private var metricRetention: MetricRetentionResult? = null

    /** 마지막 5분 평균 바퀴의 결과(성공·실패) — 뜬 뒤 아직 돌지 않았으면 null */
    val lastMetricRollup: MetricRollupResult? get() = metricRollup
    @Volatile private var metricRollup: MetricRollupResult? = null

    /** 5분 평균이 마지막으로 **성공**한 시각 — 실패한 바퀴는 이 값을 바꾸지 않는다 */
    val lastMetricRollupSuccessAt: Instant? get() = metricRollupSuccessAt
    @Volatile private var metricRollupSuccessAt: Instant? = null

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
        metricThread = metricCollector?.let { startMetricCollector(it) }
    }

    /**
     * 지표 수집 스레드 — 로그 수집 스레드와 따로 돌아 한쪽의 예외·Error 가 다른 쪽을 멈추지 않는다.
     * 예외는 로그 수집과 같이 다시 시도한다. Error(메모리 부족 · 구현 누락의 NotImplementedError 등)는 이 스레드만 끝내고
     * 서버 로그에 남긴다 — 그 뒤 헬스의 `metrics.status` 가 `stalled` 로 보인다(로그 수집의 판정은 그대로).
     */
    private fun startMetricCollector(metricCollector: MetricCollector) = Thread({
        try {
            while (running) {
                try {
                    val handled = metricCollector.runOnce()
                    if (metricCollector.lastFailure != null) Thread.sleep(FAILURE_PAUSE_MS)
                    else if (handled == 0) Thread.sleep(IDLE_PAUSE_MS)
                } catch (_: InterruptedException) {
                    break
                } catch (error: Exception) {
                    log.error("observe:observe:metric collector loop error — will retry: {}: {}", error.javaClass.simpleName, error.message?.take(200))
                    try { Thread.sleep(FAILURE_PAUSE_MS) } catch (_: InterruptedException) { break }
                }
            }
        } catch (error: Error) {
            log.error("observe:observe:metric collector stopped: {}: {}", error.javaClass.simpleName, error.message?.take(200))
            throw error
        }
    }, "observe-metric-collector").apply { isDaemon = true; start() }

    @PreDestroy
    fun stop() {
        running = false
        thread.interrupt()
        metricThread?.interrupt()
        thread.join(5_000)
        metricThread?.join(5_000)
    }

    /** 두 삭제는 서로 따로 — 한쪽이 실패해도(예외를 삼키고 결과에 남긴다) 다른 쪽은 돈다 */
    @Scheduled(cron = "\${observe.retention-cron:0 40 3 * * *}", zone = "Asia/Seoul")
    fun deleteOldLogs() {
        val now = Instant.now()
        deleteOldLogs(now)
        deleteOldAuthAttempts(now)
        deleteOldMetrics(now)
    }

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

    /** [now] 기준 보관 기간([AuthAttemptPolicy.retention])보다 오래된 로그인 시도 기록을 지운다 — 실패는 로그와 [lastAuthRetention] 에 */
    internal fun deleteOldAuthAttempts(now: Instant): Int? {
        val attempts = authAttempts ?: return null
        return runCatching { attempts.deleteOlderThan(now.minus(authPolicy.retention)) }
            .onSuccess {
                log.info("observe:observe:deleted {} auth attempts older than {} days", it, authPolicy.retention.toDays())
                authRetention = RetentionResult(now, it, null)
            }
            .onFailure {
                log.error("observe:observe:auth retention failed: {}: {}", it.javaClass.simpleName, it.message?.take(200))
                authRetention = RetentionResult(now, null, it.javaClass.simpleName)
            }
            .getOrNull()
    }

    /** 예약이 부르는 5분 평균 — 지금 시각으로 [rollUpMetrics] */
    @Scheduled(fixedDelayString = "\${observe.metrics.rollup-interval-ms:60000}")
    fun rollUpMetrics() {
        rollUpMetrics(Instant.now())
    }

    /**
     * [now] 로 5분 평균 한 바퀴([MetricMaintenance.rollUp]). 결과는 [lastMetricRollup]·[lastMetricRollupSuccessAt] 와 로그에 —
     * 실패는 예외를 삼키고 종류 이름만 남긴다. [metrics] 가 없으면 null.
     */
    internal fun rollUpMetrics(now: Instant): Int? {
        val maintenance = metrics ?: return null
        // 구간마다 따로(§16) — 실패한 구간은 세어 남기고 다음 바퀴에 다시 해 본다. 바퀴 밖의 실패(DB 불통 등)만 예외다
        return runCatching { maintenance.rollUpBuckets(now) }
            .onSuccess {
                if (it.computed > 0) log.info("observe:observe:rolled up {} metric buckets", it.computed)
                metricRollup = MetricRollupResult(now, it.computed, null, failedBuckets = it.failed)
                metricRollupSuccessAt = now
            }
            .onFailure {
                log.error("observe:observe:metric rollup failed: {}: {}", it.javaClass.simpleName, it.message?.take(200))
                metricRollup = MetricRollupResult(now, null, it.javaClass.simpleName)
            }
            .getOrNull()?.computed
    }

    /** [now] 기준 지표 삭제([MetricMaintenance.deleteExpired]) — 실패는 예외를 삼키고 [lastMetricRetention] 에 종류 이름. [metrics] 가 없으면 null */
    internal fun deleteOldMetrics(now: Instant): MetricDeletion? {
        val maintenance = metrics ?: return null
        return runCatching { maintenance.deleteExpired(now) }
            .onSuccess {
                log.info("observe:observe:deleted {} metric samples older than {} days and {} rollups older than {} days",
                    it.rawDeleted, MetricStore.RAW_RETENTION.toDays(), it.rollupDeleted, MetricStore.ROLLUP_RETENTION.toDays())
                metricRetention = MetricRetentionResult(now, it, null)
            }
            .onFailure {
                log.error("observe:observe:metric retention failed: {}: {}", it.javaClass.simpleName, it.message?.take(200))
                metricRetention = MetricRetentionResult(now, null, it.javaClass.simpleName)
            }
            .getOrNull()
    }

    /** 임계값 판정 — 지금 시각으로 [judgeThresholds]. 보내기는 [dispatchAlerts] 가 따로 돈다(§15) */
    @Scheduled(fixedDelayString = "\${observe.metrics.judge-interval-ms:30000}")
    fun judgeThresholds() {
        judgeThresholds(Instant.now())
    }

    /** [now] 로 판정만 한 바퀴([ThresholdRound.judgeOnce]) — 예외를 내지 않는다. [thresholdRound] 가 없으면 null */
    internal fun judgeThresholds(now: Instant): ThresholdRoundResult? = thresholdRound?.judgeOnce(now)

    /**
     * 알림 보내기(§15 · §15.1 — 판정과 따로 돈다. 환경 변수 `ALERT_DISPATCH_INTERVAL_MS`). 예약 실행기는 여러 스레드라
     * ([ObserveConfiguration.taskScheduler]) 한 건에 3초씩 걸리는 보내기 동안에도 판정 · 5분 평균 · 삭제가 제 주기에 돈다.
     */
    @Scheduled(fixedDelayString = "\${observe.alert.dispatch-interval-ms:30000}")
    fun dispatchAlerts() {
        dispatchAlerts(Instant.now())
    }

    /** [now] 로 보내기 한 바퀴([ThresholdRound.dispatchOnce]) — 예외를 내지 않는다. [thresholdRound] 가 없으면 null */
    internal fun dispatchAlerts(now: Instant): DispatchOutcome? = thresholdRound?.dispatchOnce(now)

    companion object {
        val RETENTION: Duration = Duration.ofDays(30)
        private const val FAILURE_PAUSE_MS = 5_000L
        private const val IDLE_PAUSE_MS = 50L
    }
}
