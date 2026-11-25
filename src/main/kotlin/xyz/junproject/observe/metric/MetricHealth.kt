package xyz.junproject.observe.metric

import org.slf4j.LoggerFactory
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import xyz.junproject.observe.threshold.ThresholdRound

/**
 * 지표 수집의 헬스 칸 — `/health` 본문의 `metrics` 객체. **로그 수집의 `status` 와 HTTP 상태에는 넣지 않는다**(§10.3).
 * 실패 사유는 예외 종류 이름만(헬스는 인증이 없다).
 *
 * `metrics` 객체의 칸(값이 없으면 빈 문자열 · -1 — 기존 칸과 같은 관례):
 * - `status`: 로그의 판정과 같은 낱말과 순서 — `unreachable`(지금 Redis 의 미확인 건수나 DB 의 마지막 저장 시각을 묻지 못함,
 *   표 없음 42P01 은 닿은 것으로 본다) → `failing`(마지막 바퀴 실패) → `ok`(마지막 성공 바퀴가 [stallAfter] 안) →
 *   `starting`(뜬 지 [stallAfter] 안, 아직 성공 없음) → `stalled`.
 * - `last_loop_at` · `last_stored_at`(DB 의 가장 최근 항목의 수신 시각) · `stored_since_start` · `unacknowledged` · `lag` · `failure`
 * - `dropped`: 이유 글자([MetricDropReason.wireName]) → 건수(모든 이유, 없으면 0) · `dropped_total` · `skipped_other_kind`
 * - `last_received_at`: 서버 이름 → 마지막 수신 시각(ISO) — 읽지 못하면 빈 객체
 * - `last_rollup_at`(5분 평균의 마지막 **성공** 시각) · `last_rollup_failure`(마지막 바퀴가 실패했으면 그 종류, 성공이면 빈 문자열)
 * - `last_retention_at` · `last_retention_raw_deleted` · `last_retention_rollup_deleted` · `last_retention_failure`
 * - `last_rollup_failed_buckets`: 마지막 5분 평균 바퀴에서 계산에 실패한 구간 수([MetricRollupResult.failedBuckets], 아직 안 돌았으면 -1)
 *
 * 헬스의 응답 시간(§15): 바깥에 묻는 것(Redis 의 미확인 · 밀린 건수, DB 의 마지막 저장 시각 · 서버별 수신 시각 · 최근 항목)은 한꺼번에 시작해
 * 합쳐 [readBudget](2초) 안에서만 기다린다. 미확인 건수나 마지막 저장 시각을 그 안에 못 얻으면 `status` 는 `unreachable`
 * (`failure` 는 `redis TimeoutException` · `database TimeoutException`), 그 밖의 칸은 "읽지 못함"(-1 · 빈 값)이다.
 * 판정 · 알림 칸은 DB 를 읽지 않는다(바퀴가 세어 둔 값).
 */
class MetricHealth(
    private val collector: MetricCollector,
    private val unacknowledged: () -> Long,
    private val lag: () -> Long?,
    private val latestStoredAt: () -> Instant?,
    private val lastReceivedByServer: () -> Map<String, Instant>,
    private val lastRollup: () -> MetricRollupResult?,
    private val lastRollupSuccessAt: () -> Instant?,
    private val lastRetention: () -> MetricRetentionResult?,
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
    /**
     * 임계값 판정 · 보내기. 있으면 `metrics` 객체에 칸을 더한다: `last_judged_at`(마지막으로 판정한 시각, 없으면 빈 글자) ·
     * `last_judge_failure` · `last_dispatch_failure`(마지막 바퀴의 예외 종류, 없으면 빈 글자) · `undelivered_alerts` · `rejected_alerts` ·
     * `skipped_unknown_rule_alerts`(못 보낸 줄 가운데 이 판이 모르는 규칙 글자라 건너뛰는 줄 — 읽지 못하면 -1).
     * 판정 · 보내기의 실패는 `metrics.status` 를 바꾸지 않는다. 없으면 칸도 없다(기존 칸 그대로).
     */
    private val thresholds: ThresholdRound? = null,
    /**
     * §15.1: 서버마다 측정 시각이 가장 늦은 항목(헬스용 연결로 짧게 읽는다). 있으면 `clock_behind` 칸 —
     * 그 항목의 수신 − 측정이 [ClockBehind.LIMIT] 을 넘는 서버 이름(이름순). 처음 그렇게 된 서버는 로그 1줄, 풀리면 1줄.
     * 읽지 못하면 빈 목록. [thresholds] 가 있으면 `silent_judging`(지금 끊김을 판정하는가)도 더한다.
     */
    private val latestSamples: (() -> List<ResourceSample>)? = null,
    /** 지표 칸이 헬스에 더하는 시간의 상한(§15) — 바깥에 묻는 것 전부를 합쳐 */
    private val readBudget: Duration = Duration.ofSeconds(2),
) {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var lastLoggedProblem: String? = null
    /** 시계가 뒤처진 것으로 마지막에 본 서버 — 바뀔 때만 로그를 남긴다 */
    private var clockBehindSeen: Set<String> = emptySet()
    private val clockBehindLock = Any()

    /** `metrics` 객체 — 예외를 내지 않는다(묻지 못한 것은 위 규칙대로 칸에 남긴다) */
    fun report(now: Instant): Map<String, Any> {
        val lastLoopAt = collector.lastLoopAt
        val failure = collector.lastFailure
        // 바깥에 묻는 것은 모두 한꺼번에 시작해 합쳐 [readBudget] 안에서만 기다린다(§15 — 지표 칸이 헬스에 더하는 시간의 상한).
        // 시간 안에 답하지 못한 것은 실패와 같다: 미확인 건수 · 마지막 저장 시각이면 `unreachable`, 그 밖의 칸은 "읽지 못함"
        val deadline = System.nanoTime() + readBudget.toNanos()
        val pendingRead = startRead(unacknowledged)
        val storedRead = startRead(latestStoredAt)
        val lagRead = startRead(lag)
        val receivedRead = startRead(lastReceivedByServer)
        val latestRead = latestSamples?.let { startRead(it) }
        val pending = awaitRead(pendingRead, deadline)
        val stored = awaitRead(storedRead, deadline).recoverCatching { if (isTableMissing(it)) null else throw it }
        val problems = listOfNotNull(pending.exceptionOrNull()?.let { "redis" to it }, stored.exceptionOrNull()?.let { "database" to it })
        logProblems(problems)
        val status = when {
            problems.isNotEmpty() -> "unreachable"
            failure != null -> "failing"
            lastLoopAt != null && Duration.between(lastLoopAt, now) < stallAfter -> "ok"
            lastLoopAt == null && Duration.between(startedAt, now) < stallAfter -> "starting"
            else -> "stalled"
        }
        val dropped = collector.droppedCounts()
        val rollup = runCatching(lastRollup).getOrNull()
        val retention = runCatching(lastRetention).getOrNull()
        // 판정 · 알림 칸 — 바퀴가 세어 둔 값만 읽는다(판정 · 보내기용 연결을 쓰지도, 표를 만들지도 않는다)
        val judging = thresholds?.let { round ->
            val counts = round.lastAlertCounts
            mapOf(
                "last_judged_at" to (round.lastJudgedAt?.toString() ?: ""),
                "last_judge_failure" to (round.lastResult?.judgeFailure ?: ""),
                "last_dispatch_failure" to (round.lastDispatchFailure ?: ""),
                "undelivered_alerts" to (counts?.undelivered ?: -1L),
                "rejected_alerts" to (counts?.rejected ?: -1L),
                "skipped_unknown_rule_alerts" to (counts?.unknownRule ?: -1L),
                "silent_judging" to runCatching { round.silentJudging(now) }.getOrDefault(false),
            )
        }.orEmpty()
        val received = awaitRead(receivedRead, deadline).getOrNull()
        val clockBehind = latestRead?.let { read -> mapOf("clock_behind" to clockBehind(awaitRead(read, deadline).getOrNull())) }.orEmpty()
        return judging + clockBehind + mapOf(
            "status" to status,
            "last_loop_at" to (lastLoopAt?.toString() ?: ""),
            "last_stored_at" to (stored.getOrNull()?.toString() ?: ""),
            "stored_since_start" to collector.storedTotal,
            "unacknowledged" to (pending.getOrNull() ?: -1L),
            "lag" to (awaitRead(lagRead, deadline).getOrNull() ?: -1L),
            "failure" to (problems.joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}" }.ifEmpty { failure ?: "" }),
            "dropped" to MetricDropReason.entries.associate { it.wireName to (dropped[it] ?: 0L) },
            "dropped_total" to dropped.values.sum(),
            "skipped_other_kind" to collector.skippedOtherKind,
            "last_received_at" to (received?.mapValues { it.value.toString() } ?: emptyMap<String, String>()),
            "last_rollup_at" to (runCatching(lastRollupSuccessAt).getOrNull()?.toString() ?: ""),
            "last_rollup_failure" to (rollup?.failure ?: ""),
            "last_rollup_failed_buckets" to (rollup?.failedBuckets ?: -1),
            "last_retention_at" to (retention?.ranAt?.toString() ?: ""),
            "last_retention_raw_deleted" to (retention?.deletion?.rawDeleted ?: -1),
            "last_retention_rollup_deleted" to (retention?.deletion?.rollupDeleted ?: -1),
            "last_retention_failure" to (retention?.failure ?: ""),
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

    /** 시계가 뒤처진 서버(이름순) — 읽지 못했으면 빈 목록이고 로그 기준도 바꾸지 않는다. 새로 뒤처진 서버 · 풀린 서버만 1줄씩 */
    private fun clockBehind(latest: List<ResourceSample>?): List<String> {
        if (latest == null) return emptyList()
        val behind = ClockBehind.servers(latest)
        synchronized(clockBehindLock) {
            val now = behind.toSet()
            for (server in now - clockBehindSeen) {
                val sample = latest.first { it.server == server }
                log.warn("observe:observe:server clock behind: {} (received {} after measured)", server, Duration.between(sample.measuredAt, sample.receivedAt))
            }
            for (server in clockBehindSeen - now) log.info("observe:observe:server clock caught up: {}", server)
            clockBehindSeen = now
        }
        return behind
    }

    private fun isTableMissing(error: Throwable) =
        generateSequence(error) { it.cause }.any { it is SQLException && it.sqlState == UNDEFINED_TABLE }

    /** 닿지 못한 이유의 전체 문구는 서버 로그에만 — 같은 이유를 헬스 요청마다 되풀이해 남기지 않는다 */
    private fun logProblems(problems: List<Pair<String, Throwable>>) {
        val detail = problems.joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}: ${error.message?.take(200)}" }.ifEmpty { null }
        if (detail != lastLoggedProblem && detail != null) log.warn("observe:observe:metric health check unreachable: {}", detail)
        lastLoggedProblem = detail
    }

    private companion object {
        /** 헬스의 선택 칸 읽기 — 가상 스레드라 늦은 읽기가 쌓여도 스레드를 잡지 않는다 */
        val READERS = Executors.newVirtualThreadPerTaskExecutor()!!
        const val UNDEFINED_TABLE = "42P01"   // PostgreSQL: relation does not exist
    }
}
