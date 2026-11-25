package xyz.junproject.observe.collect

import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.RetentionResult
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.traffic.TrafficHealth
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors

/**
 * 수집이 살아 있는가의 판정. 200 은 `ok` 하나뿐이다.
 * - `unreachable`(503): 지금 Redis(미확인 건수)나 DB(가장 최근 행의 시각)에 닿지 못한다. 표가 아직 없다는 답(SQLSTATE 42P01)은
 *   DB 에 닿은 것이라 여기 넣지 않는다 — 스키마를 만들기 전(`starting`)이나 만들지 못한 때(`failing`, `schema …`)를 가리지 않게.
 *   헬스를 물을 때마다 직접 보고
 *   결과를 담아 두지 않는다 — 스트림이 조용하면 수집 바퀴가 DB 에 닿지 않아 바퀴 결과만으로는 "DB 가 죽었는데 정상"으로 보이고,
 *   담아 두면 끊긴 직후에도 200 이 나간다.
 * - `failing`(503): 마지막 수집 바퀴가 실패했다(스키마 생성 실패 포함).
 * - `ok`(200): 마지막으로 성공한 바퀴가 [stallAfter] 안이다(읽을 것이 없었어도).
 * - `starting`(503): 뜬 지 [stallAfter] 가 안 됐고 아직 한 바퀴도 성공하지 못했다. 배포의 헬스 게이트는 첫 200 에서 통과하므로
 *   한 번도 수집하지 못한 자리를 통과시키지 않게 503 이다.
 * - `stalled`(503): 그 밖 — 바퀴가 [stallAfter] 넘게 끝나지 않는다(첫 바퀴 전 포함).
 * 밀린 건수(lag)와 30일 삭제·로그인 시도 90일 삭제 결과는 본문에만 낸다 — 밀림은 장애가 아니라 진행 상황이고, 삭제 실패는 수집과 무관하다.
 * 실패 사유는 예외 종류 이름만 낸다(헬스는 인증이 없다). 문구는 서버 로그에만.
 * [metrics] 가 있으면 본문에 `metrics` 객체([MetricHealth.report])를, [traffic] 이 있으면 `traffic` 객체([TrafficHealth.report])를 더한다 —
 * 그쪽이 어떻게 실패해도 위 `status` 와 HTTP 상태는 바뀌지 않는다(만들다 실패하면 `{status:"failing", failure:"health <종류>"}`).
 */
class CollectorHealth(
    private val collector: LogCollector,
    private val unacknowledged: () -> Long,           // 받았지만 확인하지 못한 건수 — 물어볼 수 없으면 예외(= Redis 에 닿지 못함)
    private val lag: () -> Long?,                     // 그룹에 아직 전달되지 않은 건수 — 셀 수 없으면 null, 실패해도 판정에 넣지 않는다
    private val latestStoredAt: () -> Instant?,       // DB 의 가장 최근 행의 시각(비었으면 null) — 닿지 못하면 예외
    private val lastRetention: () -> RetentionResult?,
    private val lastAuthRetention: () -> RetentionResult? = { null },   // 로그인 시도 기록 90일 삭제 — 본문에만, 판정에 넣지 않는다
    private val stallAfter: Duration = Duration.ofSeconds(60),
    private val startedAt: Instant = Instant.now(),
    private val metrics: MetricHealth? = null,                          // 지표 수집의 칸 — 본문에만, 판정에 넣지 않는다
    private val traffic: TrafficHealth? = null,                         // 통신 수집의 칸 — 본문에만, 판정에 넣지 않는다
) {
    data class Report(val healthy: Boolean, val body: Map<String, Any>)

    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var lastLoggedProblem: String? = null

    fun report(now: Instant): Report {
        // 지표 칸은 로그 쪽 읽기와 동시에 시작한다(§16) — 헬스 전체는 둘의 합이 아니라 긴 쪽(지표 칸은 그 안에서 최대 2초)
        val metricReport = metrics?.let { metricHealth -> CompletableFuture.supplyAsync({ metricHealth.report(now) }, SECTION_READER) }
        val trafficReport = traffic?.let { trafficHealth -> CompletableFuture.supplyAsync({ trafficHealth.report(now) }, SECTION_READER) }
        val lastLoopAt = collector.lastLoopAt
        val failure = collector.lastFailure
        val pending = runCatching(unacknowledged)
        // 표가 없으면 DB 에는 닿았고 저장된 행이 없는 것과 같다 — 마지막 저장 시각은 비우고 판정은 수집 바퀴의 결과에 맡긴다
        val stored = runCatching(latestStoredAt).recoverCatching { if (isTableMissing(it)) null else throw it }
        val behind = runCatching(lag)
        val problems = listOfNotNull(pending.exceptionOrNull()?.let { "redis" to it }, stored.exceptionOrNull()?.let { "database" to it })
        logProblems(problems)
        val status = when {
            problems.isNotEmpty() -> "unreachable"
            failure != null -> "failing"
            lastLoopAt != null && Duration.between(lastLoopAt, now) < stallAfter -> "ok"
            lastLoopAt == null && Duration.between(startedAt, now) < stallAfter -> "starting"
            else -> "stalled"
        }
        val retention = lastRetention()
        val authRetention = lastAuthRetention()
        val body = mapOf(
            "status" to status,
            "last_loop_at" to (lastLoopAt?.toString() ?: ""),
            "last_stored_at" to (stored.getOrNull()?.toString() ?: ""),     // DB 의 가장 최근 로그가 찍힌 시각
            "stored_since_start" to collector.storedTotal,
            "unacknowledged" to (pending.getOrNull() ?: -1L),
            "lag" to (behind.getOrNull() ?: -1L),
            "failure" to (problems.joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}" }.ifEmpty { failure ?: "" }),
            "last_retention_at" to (retention?.ranAt?.toString() ?: ""),
            "last_retention_deleted" to (retention?.deleted ?: -1),
            "last_retention_failure" to (retention?.failure ?: ""),
            "last_auth_retention_at" to (authRetention?.ranAt?.toString() ?: ""),
            "last_auth_retention_deleted" to (authRetention?.deleted ?: -1),
            "last_auth_retention_failure" to (authRetention?.failure ?: ""),
        )
        // 지표 · 통신 칸은 판정이 끝난 뒤에 덧붙인다 — 그쪽의 어떤 실패도 위 status 와 HTTP 상태에 닿지 않게(만들다 실패해도 칸에만 남긴다)
        val sections = listOfNotNull(metricReport?.let { "metrics" to it }, trafficReport?.let { "traffic" to it }).associate { (name, report) ->
            name to runCatching { report.get() }.getOrElse {
                val cause = (it as? ExecutionException)?.cause ?: it
                log.error("observe:observe:{} health failed: {}", name, cause.javaClass.simpleName)
                mapOf("status" to "failing", "failure" to "health ${cause.javaClass.simpleName}")
            }
        }
        return Report(status == "ok", body + sections)
    }

    private fun isTableMissing(error: Throwable) = HealthReads.isTableMissing(error)

    /** 닿지 못한 이유의 전체 문구는 서버 로그에만 — 같은 이유를 헬스 요청마다 되풀이해 남기지 않는다 */
    private fun logProblems(problems: List<Pair<String, Throwable>>) {
        val detail = problems.joinToString("; ") { (where, error) -> "$where ${error.javaClass.simpleName}: ${error.message?.take(200)}" }.ifEmpty { null }
        if (detail != lastLoggedProblem && detail != null) log.warn("observe:observe:health check unreachable: {}", detail)
        lastLoggedProblem = detail
    }

    companion object {
        /** 지표 · 통신 칸을 읽는 가상 스레드 — 그 읽기는 각 칸이 스스로 2초에서 끊는다 */
        private val SECTION_READER = Executors.newVirtualThreadPerTaskExecutor()
    }
}
