package xyz.junproject.observe.visit

import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.MetricStreamRecord
import xyz.junproject.observe.metric.MetricStreamSource
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicLong

/** 수집 쪽이 쓰는 면 */
interface VisitSink {
    /** 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema()
    /** 묶음을 저장한다. 이미 있는 항목(같은 스트림 ID)은 건너뛴다. 새로 들어간 줄의 스트림 ID 를 돌려준다(이미 있던 것은 빠진다). 실패는 예외 */
    fun insertAll(visits: List<Visit>): Set<StreamId>
}

/**
 * 방문 스트림(`visits`)의 수집 — 지표 수집([xyz.junproject.observe.metric.MetricCollector])과 같은 규칙의 한 바퀴.
 * - 받아들인 것을 저장한 **뒤에만** 확인한다. 저장이 실패하면 확인하지 않는다(다음 바퀴에 다시 받는다 — 잃지 않는다).
 * - 꼴이 틀린 항목은 `<스트림 ID>: <이유>` 만 로그에 남기고 [droppedCounts] 를 센 뒤 확인한다(다시 받지 않는다). 같은 묶음의 다른 항목은 저장한다.
 * - 경로의 `?` · `#` 뒤를 잘라 낸 항목은 저장과 확인이 끝난 뒤에 [queryStripped] 를 센다(다시 받아 두 번 세지 않게).
 * - **주소 원문을 어디에도 내지 않는다**: 실패의 로그와 [lastFailure] 에는 예외의 종류 이름(과 SQLSTATE)만 — PostgreSQL 드라이버의
 *   묶음 저장 오류 글에는 넣으려던 값(주소 · 구분값)이 실린다.
 */
class VisitCollector(
    private val source: MetricStreamSource,
    private val sink: VisitSink,
    private val batchSize: Int = 200,
    private val block: Duration = Duration.ofSeconds(2),
    private val recheckUnacknowledgedEvery: Duration = Duration.ofMinutes(1),
    private val clock: () -> Instant = Instant::now,
) {
    private val log = LoggerFactory.getLogger("observe")
    private var schemaReady = false
    private var groupReady = false
    private var lastUnacknowledgedCheck: Instant? = null
    private var lastFailureDetail: String? = null                  // 같은 실패를 바퀴마다 되풀이해 로그에 남기지 않으려고
    private val dropped = EnumMap<VisitDropReason, AtomicLong>(VisitDropReason::class.java)
        .apply { VisitDropReason.entries.forEach { put(it, AtomicLong()) } }

    /** 마지막으로 한 바퀴를 끝까지 돈 시각(읽을 것이 없었어도) */
    @Volatile var lastLoopAt: Instant? = null; private set
    /** 마지막 바퀴가 실패했으면 예외 종류 이름(스키마 단계면 앞에 "schema "), 성공했으면 null */
    @Volatile var lastFailure: String? = null; private set
    /** 이 프로세스가 뜬 뒤 저장한 항목 수 */
    @Volatile var storedTotal: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 경로의 `?` · `#` 뒤를 잘라 내고 저장한 항목 수 */
    @Volatile var queryStripped: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 받았지만 표에 이미 있어(같은 스트림 ID) 넣지 않은 항목 수 — 뼈대: 구현 워커가 센다 */
    @Volatile var alreadyStored: Long = 0; private set

    /** 이 프로세스가 뜬 뒤 버린 항목 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun droppedCounts(): Map<VisitDropReason, Long> = dropped.mapValues { it.value.get() }

    /** 한 바퀴. 처리한(저장하거나 버리고 확인한) 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다. */
    fun runOnce(): Int {
        var step = "collect"
        return try {
            // 그룹을 먼저, 스키마는 DB 에 처음 닿았을 때 — 로그 · 지표 수집과 같은 순서와 까닭
            if (!groupReady) { source.ensureGroup(); groupReady = true }
            if (!schemaReady) { step = "schema"; sink.createSchema(); schemaReady = true; step = "collect"; log.info("observe:observe:visit schema ready") }
            collect()
        } catch (error: Exception) {
            // 확인하지 않고 물러난다 — 다음 바퀴에 미확인분부터 다시 받고, 사라졌을 수 있는 그룹(Redis 재시작)도 다시 만든다
            lastUnacknowledgedCheck = null
            groupReady = false
            val kind = error.javaClass.simpleName
            val detail = "$step $kind${sqlState(error)?.let { " (SQLSTATE $it)" } ?: ""}"   // 예외 글은 싣지 않는다 — 주소가 들어 있을 수 있다
            if (detail != lastFailureDetail) log.error("observe:observe:visit collect failed — will retry: {}", detail)
            lastFailureDetail = detail
            lastFailure = if (step == "schema") "schema $kind" else kind
            0
        }
    }

    private fun collect(): Int {
        val now = clock()
        var handled = 0
        val checkedAt = lastUnacknowledgedCheck
        if (checkedAt == null || !now.isBefore(checkedAt.plus(recheckUnacknowledgedEvery))) {
            while (true) {
                val leftover = source.readUnacknowledged(batchSize)
                if (leftover.isEmpty()) break
                handled += storeAndAcknowledge(leftover)
            }
            lastUnacknowledgedCheck = now
        }
        handled += storeAndAcknowledge(source.readNew(batchSize, block))
        lastLoopAt = clock()
        lastFailure = null
        lastFailureDetail = null
        return handled
    }

    private fun storeAndAcknowledge(records: List<MetricStreamRecord>): Int {
        if (records.isEmpty()) return 0
        val accepted = ArrayList<Visit>(records.size)
        val acceptedIds = ArrayList<String>(records.size)
        var strippedNow = 0
        val droppedNow = ArrayList<Pair<String, VisitDropReason>>()
        for (record in records) {
            when (val parsed = VisitEntryParser.parse(record)) {
                is VisitEntryParse.Accepted -> { accepted += parsed.visit; acceptedIds += record.id; if (parsed.queryStripped) strippedNow++ }
                is VisitEntryParse.Dropped -> droppedNow += record.id to parsed.reason
            }
        }
        // 버린 것은 확인에 성공한 뒤에 센다 — 확인이 실패하면 다시 받으므로 그때 한 번만 세게
        if (droppedNow.isNotEmpty()) {
            source.acknowledge(droppedNow.map { it.first })
            for ((id, reason) in droppedNow) {
                dropped.getValue(reason).incrementAndGet()
                log.warn("observe:observe:dropped visit entry {}: {}", id.take(40), reason.wireName)   // 항목의 칸은 찍지 않는다
            }
        }
        if (accepted.isNotEmpty()) {
            sink.insertAll(accepted)                     // 실패하면 예외 → 아래 확인을 하지 않는다(다음 바퀴에 다시 받는다)
            source.acknowledge(acceptedIds)
            storedTotal += accepted.size
            queryStripped += strippedNow
        }
        return records.size
    }

    private fun sqlState(error: Throwable): String? =
        generateSequence(error) { it.cause }.filterIsInstance<SQLException>().firstNotNullOfOrNull { it.sqlState }
}
