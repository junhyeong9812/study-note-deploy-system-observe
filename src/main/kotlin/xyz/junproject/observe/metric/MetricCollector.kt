package xyz.junproject.observe.metric

import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicLong

/** 지표 스트림을 소비자 그룹으로 읽는 면 — 로그의 [xyz.junproject.observe.collect.LogStreamSource] 와 같은 규칙, 항목은 칸 전부 */
interface MetricStreamSource {
    /** 소비자 그룹이 없으면 만든다(스트림의 처음부터 읽게) */
    fun ensureGroup()
    /** 받았지만 아직 확인(ack)하지 못한 항목 */
    fun readUnacknowledged(count: Int): List<MetricStreamRecord>
    /** 아직 아무에게도 가지 않은 새 항목. 없으면 [block] 만큼 기다린다 */
    fun readNew(count: Int, block: Duration): List<MetricStreamRecord>
    fun acknowledge(ids: List<String>)
    /** 받았지만 확인하지 못한 항목 수 */
    fun unacknowledgedCount(): Long
}

/**
 * 지표 수집 — 로그 수집([xyz.junproject.observe.collect.LogCollector])과 따로 도는 전용 스레드의 한 바퀴.
 * - 항목마다 [MetricEntryParser.parse] (수신 시각 = [clock]). 받아들인 것을 저장한 **뒤에만** 확인한다.
 * - 꼴이 틀린 항목은 이유와 항목 ID 를 로그에 남기고 [droppedCounts] 를 센 뒤 확인한다(다시 받지 않는다).
 *   다른 종류(`kind` ≠ `resource`)는 [skippedOtherKind] 를 세고 확인한다. 둘 다 같은 묶음의 다른 항목 저장을 막지 않는다.
 * - 저장(DB)이 실패하면 받아들인 항목을 확인하지 않는다(다음 바퀴에 다시 받는다). 버린 항목 · 다른 종류는 DB 와 무관하므로
 *   저장의 성패와 상관없이 확인한다 — 다시 받아 두 번 세지 않게.
 * - 미확인분은 기동 직후와 [recheckUnacknowledgedEvery] 마다 먼저 받는다. 실패하면 다음 바퀴에 그룹을 다시 확인한다.
 */
class MetricCollector(
    private val source: MetricStreamSource,
    private val sink: MetricSink,
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
    private val dropped = EnumMap<MetricDropReason, AtomicLong>(MetricDropReason::class.java)
        .apply { MetricDropReason.entries.forEach { put(it, AtomicLong()) } }

    /** 마지막으로 한 바퀴를 끝까지 돈 시각(읽을 것이 없었어도) */
    @Volatile var lastLoopAt: Instant? = null; private set
    /** 마지막 바퀴가 실패했으면 예외 종류 이름(스키마 단계면 앞에 "schema "), 성공했으면 null — 문구는 서버 로그에만 */
    @Volatile var lastFailure: String? = null; private set
    /** 이 프로세스가 뜬 뒤 저장한 항목 수 */
    @Volatile var storedTotal: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 다른 종류라서 건너뛴 항목 수 */
    @Volatile var skippedOtherKind: Long = 0; private set

    /** 이 프로세스가 뜬 뒤 버린 항목 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun droppedCounts(): Map<MetricDropReason, Long> = dropped.mapValues { it.value.get() }

    /** 한 바퀴. 처리한(저장하거나 버리거나 건너뛰고 확인한) 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다. */
    fun runOnce(): Int {
        var step = "collect"
        return try {
            // 그룹을 먼저, 스키마는 DB 에 처음 닿았을 때 — 로그 수집([xyz.junproject.observe.collect.LogCollector])과 같은 순서와 까닭
            if (!groupReady) { source.ensureGroup(); groupReady = true }
            if (!schemaReady) { step = "schema"; sink.createSchema(); schemaReady = true; step = "collect"; log.info("observe:observe:metric schema ready") }
            collect()
        } catch (error: Exception) {
            // 확인하지 않고 물러난다 — 다음 바퀴에 미확인분부터 다시 받고, 사라졌을 수 있는 그룹(Redis 재시작)도 다시 만든다
            lastUnacknowledgedCheck = null
            groupReady = false
            val kind = error.javaClass.simpleName
            val detail = "$step $kind: ${error.message?.take(200)}"
            if (detail != lastFailureDetail) log.error("observe:observe:metric collect failed — will retry: {}", detail)
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
        val receivedAt = clock()
        val accepted = ArrayList<ResourceSample>(records.size)
        val acceptedIds = ArrayList<String>(records.size)
        val settledIds = ArrayList<String>()                                    // 버린 것 · 다른 종류 — DB 와 무관하게 확인한다
        val droppedNow = ArrayList<Pair<MetricStreamRecord, MetricDropReason>>()
        var otherKinds = 0
        for (record in records) {
            when (val parsed = MetricEntryParser.parse(record, receivedAt)) {
                is MetricEntryParse.Accepted -> { accepted += parsed.sample; acceptedIds += record.id }
                is MetricEntryParse.OtherKind -> { settledIds += record.id; otherKinds++ }
                is MetricEntryParse.Dropped -> { settledIds += record.id; droppedNow += record to parsed.reason }
            }
        }
        // 버린 것은 확인에 성공한 뒤에 센다 — 확인이 실패하면 다시 받으므로 그때 한 번만 세게
        if (settledIds.isNotEmpty()) {
            source.acknowledge(settledIds)
            for ((record, reason) in droppedNow) {
                dropped.getValue(reason).incrementAndGet()
                log.warn("observe:observe:dropped metric entry {}: {}", record.id.take(40), reason.wireName)
            }
            skippedOtherKind += otherKinds
        }
        if (accepted.isNotEmpty()) {
            sink.insertAll(accepted)                     // 실패하면 예외 → 아래 확인을 하지 않는다(다음 바퀴에 다시 받는다)
            source.acknowledge(acceptedIds)
            storedTotal += accepted.size
        }
        return records.size
    }
}
