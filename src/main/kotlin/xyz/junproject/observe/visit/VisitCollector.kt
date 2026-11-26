package xyz.junproject.observe.visit

import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.MetricStreamRecord
import xyz.junproject.observe.metric.MetricStreamSource
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import java.util.Collections
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 묶음 저장의 결과 — [inserted] 는 새로 넣은 줄의 스트림 ID, [resultUnknown] 은 드라이버가 "결과 모름"(`Statement.SUCCESS_NO_INFO`)을 돌려준 항목.
 * 둘 다 아닌 항목은 표에 이미 있던 것이다.
 */
data class VisitInsertion(val inserted: Set<StreamId>, val resultUnknown: Set<StreamId> = emptySet())

/** 수집 쪽이 쓰는 면 */
interface VisitSink {
    /** 표를 만든다(있으면 그대로). 실패는 예외 */
    fun createSchema()
    /** 묶음을 저장한다. 이미 있는 항목(같은 스트림 ID)은 건너뛴다. 새로 들어간 줄의 스트림 ID 를 돌려준다(이미 있던 것은 빠진다). 실패는 예외 */
    fun insertAll(visits: List<Visit>): VisitInsertion
}

/**
 * 방문 스트림(`visits`)의 수집 — 지표 수집([xyz.junproject.observe.metric.MetricCollector])과 같은 규칙의 한 바퀴.
 * - 받아들인 것을 저장한 **뒤에만** 확인한다. 저장이 실패하면 확인하지 않는다(다음 바퀴에 다시 받는다 — 잃지 않는다).
 * - 꼴이 틀린 항목은 `<스트림 ID>: <이유>` 만 로그에 남기고 [droppedCounts] 를 센 뒤 확인한다(다시 받지 않는다). 같은 묶음의 다른 항목은 저장한다.
 * - 저장 수 · 경로의 `?` · `#` · `;` 뒤를 잘라 낸 수는 저장소가 새로 넣었다고 돌려준 항목만, 저장과 확인이 끝난 뒤에 센다(다시 받아 두 번 세지 않게).
 *   표에 이미 있던 항목(앞 프로세스가 저장하고 확인 전에 죽음)은 [alreadyStored] 로 따로 센다.
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
    /**
     * 저장 · 확인을 마친 항목을 지우는 면 — 하루 한 번의 정리([VisitSchedule])도 이것을 쓴다. null 이면 지우지 않는다(지금 동작).
     * 차례: 받아들인 묶음은 저장 → 확인 → 지움, 꼴이 틀려 버린 항목은 확인 → 지움. 저장에 실패한 묶음은 확인도 지우기도 하지 않는다.
     * 지우기가 실패하면 바퀴는 실패가 아니다 — [streamDeleteFailed] 에 그 수를 더하고 ID 를 기억했다가 **다음 바퀴의 처음에** 다시 지운다(다시 저장하지 않는다).
     * 로그에는 `visit stream delete failed for <n> entries — will retry: <종류>` 만(예외 글 · ID 없음).
     */
    val cleaner: VisitStreamCleaner? = null,
    /**
     * 뼈대(V2b 리뷰 루프 1): 다시 지울 ID 를 기억하는 상한(기본 1만) — 지우기가 실패해 목록에 더하다 넘으면 **가장 오래된 것부터** 잊고
     * [streamDeleteForgotten] 에 센다(잊은 항목은 하루 한 번의 안전망이 거둔다). 넘친 바퀴마다 WARN 한 줄(ID · 주소 없음):
     * `observe:observe:visit stream delete pending list over <상한> — forgot the <그 바퀴에 잊은 수> oldest entries, the daily trim removes them`.
     * 본문은 구현 워커가 채운다.
     */
    @Suppress("unused") private val deletePendingLimit: Int = 10_000,
    /**
     * 뼈대(V2b 리뷰 루프 1): 바퀴 처음의 다시 지우기를 한 번에 몇 개씩(기본 1천) — 오래된 것부터 묶음으로, 한 묶음이 실패하면 그 바퀴의 나머지 묶음은 하지 않는다.
     * 성공한 묶음의 ID 만 목록에서 빠진다. 본문은 구현 워커가 채운다.
     */
    @Suppress("unused") private val deleteRetryBatchSize: Int = 1_000,
) {
    /** 뼈대(V2b 리뷰 루프 1): 이 프로세스가 뜬 뒤 상한을 넘어 다시 지우기를 포기한(잊은) 항목 수 — 헬스 `visits.stream_delete_forgotten` */
    @Volatile var streamDeleteForgotten: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 스트림에서 지운 항목 수(Redis 가 지웠다고 돌려준 수) */
    @Volatile var streamDeleted: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 지우기에 실패한 항목 수(실패한 시도마다 그 묶음의 수를 더한다) */
    @Volatile var streamDeleteFailed: Long = 0; private set
    /** 지금 다시 지우기를 기다리는 항목 수 */
    val streamDeletePending: Int get() = pendingDeletes.size
    // 단순화: 지우기에 실패한 ID 는 이 프로세스의 메모리에만 둔다 — 그 사이 프로세스가 다시 뜨면 잊고, 하루 한 번의 안전망(90일 자르기)이 거둔다.
    //        Redis 가 오래 쓰기를 거절하면 그동안 확인한 수만큼 늘어난다(하루 방문 수천 건 규모 — 넘으면 상한을 두고 안전망에 맡긴다)
    private val pendingDeletes: MutableSet<String> = Collections.synchronizedSet(LinkedHashSet())
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
    /** 이 프로세스가 뜬 뒤 저장한(표에 새로 넣은) 항목 수 */
    @Volatile var storedTotal: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 경로의 `?` · `#` · `;` 뒤를 잘라 내고 저장한(표에 새로 넣은) 항목 수 */
    @Volatile var queryStripped: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 받았지만 표에 이미 있어(같은 스트림 ID) 넣지 않은 항목 수 */
    @Volatile var alreadyStored: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 저장소가 "결과 모름"을 돌려준 항목 수 — 저장 수 · 이미 있음 · 자른 수 어디에도 세지 않는다(넣었는지 짐작하지 않는다) */
    @Volatile var storeResultUnknown: Long = 0; private set

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
        // 앞 바퀴에서 지우지 못한 항목을 먼저 — 같은 바퀴의 끝에서 다시 하면 한 번의 장애가 실패 수에 두 번 세어진다
        if (pendingDeletes.isNotEmpty()) deleteFromStream(synchronized(pendingDeletes) { pendingDeletes.toList() })
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
        val strippedIds = HashSet<StreamId>()
        val droppedNow = ArrayList<Pair<String, VisitDropReason>>()
        for (record in records) {
            when (val parsed = VisitEntryParser.parse(record)) {
                is VisitEntryParse.Accepted -> { accepted += parsed.visit; acceptedIds += record.id; if (parsed.queryStripped) strippedIds += parsed.visit.id }
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
            deleteFromStream(droppedNow.map { it.first })   // 확인한 뒤에 — 버린 항목도 주소가 든 원문이다
        }
        if (accepted.isNotEmpty()) {
            val insertion = sink.insertAll(accepted)     // 실패하면 예외 → 아래 확인을 하지 않는다(다음 바퀴에 다시 받는다)
            source.acknowledge(acceptedIds)              // 결과 모름도 확인한다 — 저장 문장은 성공했다
            val inserted = insertion.inserted
            storedTotal += inserted.size
            storeResultUnknown += accepted.count { it.id in insertion.resultUnknown }
            alreadyStored += accepted.count { it.id !in inserted && it.id !in insertion.resultUnknown }
            queryStripped += strippedIds.count { it in inserted }
            deleteFromStream(acceptedIds)                // 저장하고 확인한 뒤에만 — 저장이 실패하면 위에서 예외로 물러나 여기에 오지 않는다
        }
        return records.size
    }

    /**
     * 확인을 마친 항목을 스트림에서 지운다. 실패해도 예외를 내지 않는다(바퀴는 실패가 아니다 — 저장 · 확인은 끝났다): 그 수를 [streamDeleteFailed] 에 더하고
     * ID 를 기억했다가 다음 바퀴의 처음에 다시 지운다. 로그에는 수와 예외 종류 이름만.
     */
    private fun deleteFromStream(ids: List<String>) {
        val cleaner = cleaner ?: return
        if (ids.isEmpty()) return
        try {
            streamDeleted += cleaner.delete(ids)
            pendingDeletes.removeAll(ids.toSet())
        } catch (error: Exception) {
            streamDeleteFailed += ids.size
            pendingDeletes.addAll(ids)
            log.warn("observe:observe:visit stream delete failed for {} entries — will retry: {}", ids.size, error.javaClass.simpleName)
        }
    }

    private fun sqlState(error: Throwable): String? =
        generateSequence(error) { it.cause }.filterIsInstance<SQLException>().firstNotNullOfOrNull { it.sqlState }
}
