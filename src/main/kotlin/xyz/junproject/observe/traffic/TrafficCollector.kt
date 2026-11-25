package xyz.junproject.observe.traffic

import org.slf4j.LoggerFactory
import xyz.junproject.observe.metric.MetricStreamRecord
import xyz.junproject.observe.metric.MetricStreamSource
import xyz.junproject.observe.metric.RedisMetricStreamSource
import java.time.Duration
import java.time.Instant
import java.util.EnumMap
import java.util.concurrent.atomic.AtomicLong

/** 항목 안에서 칸 · 요청별 줄만 버린 이유 — 헬스의 `traffic.cells_dropped` 에 나가는 글자 그대로 */
enum class TrafficCellDrop(val wireName: String) {
    SEGMENT_AHEAD("segment_ahead"), SEGMENT_TOO_OLD("segment_too_old"), REQUEST_AHEAD("request_ahead"), REQUEST_TOO_OLD("request_too_old"),
}

/**
 * 통신 수집 — 로그 · 지표 수집과 따로 도는 전용 스레드(`observe-traffic-collector`)의 한 바퀴. 지표 수집([xyz.junproject.observe.metric.MetricCollector])과
 * 같은 규칙(소비자 그룹 · 미확인분 먼저 · 그룹을 먼저, 스키마는 DB 에 처음 닿을 때)을 스트림 이름만 달리해 쓴다(규약 4.1).
 * - 항목마다 [TrafficEntryParser.parse](받은 시각 = [clock]) → 받아들인 항목은 **항목마다** [TrafficSink.store] 하고, 저장한 항목만 확인한다.
 *   저장이 실패하면 그 항목과 그 뒤의 항목은 확인하지 않는다(다음 바퀴에 다시 받는다 — 이미 저장된 것은 순번으로 버려진다).
 * - 꼴이 틀린 항목은 이유를 세고 확인한다(로그에는 스트림 ID 와 이유 글자만). 다른 종류는 [skippedOtherKind] 를 세고 확인한다.
 * - 순번으로 버린 항목은 [duplicates] 에 세고, 새로 받은 항목만 [storedTotal] · 칸마다 버린 수([cellDropCounts]) · `at` 의 앞섬 · 오래됨([atAhead] · [atTooOld],
 *   경계는 [TrafficCellAge])에 센다. `at` 으로는 항목을 버리지 않는다.
 * - 로그에는 예외 종류 이름만 싣는다 — 예외 문구 · 항목의 내용을 싣지 않는다.
 * - 버린 것 · 다른 종류는 DB 와 무관하므로 저장보다 먼저 확인한다(앞 항목의 저장이 실패해도 다시 받아 두 번 세지 않게).
 */
class TrafficCollector(
    private val source: MetricStreamSource,
    private val sink: TrafficSink,
    private val batchSize: Int = 200,
    private val block: Duration = Duration.ofSeconds(2),
    private val recheckUnacknowledgedEvery: Duration = Duration.ofMinutes(1),
    private val clock: () -> Instant = Instant::now,
) {
    /** 마지막으로 한 바퀴를 끝까지 돈 시각 */
    @Volatile var lastLoopAt: Instant? = null; private set
    /** 마지막 바퀴가 실패했으면 예외 종류 이름(스키마 단계면 앞에 "schema "), 성공했으면 null */
    @Volatile var lastFailure: String? = null; private set
    /** 마지막으로 저장이 실패한 때의 예외 종류 이름과 시각 — 그 뒤에 성공해도 남는다 */
    @Volatile var lastStoreFailure: String? = null; private set
    @Volatile var lastStoreFailureAt: Instant? = null; private set
    /** 이 프로세스가 뜬 뒤 새로 저장한 항목 수(순번으로 버린 것은 빼고) */
    @Volatile var storedTotal: Long = 0; private set
    @Volatile var skippedOtherKind: Long = 0; private set
    /** 이미 받은 순번이라 통째로 버린 항목 수 */
    @Volatile var duplicates: Long = 0; private set
    /** 새로 받은 항목 가운데 `at` 이 받은 시각보다 2분 넘게 앞선 수 · 6일 넘게 뒤진 수(항목은 저장했다) */
    @Volatile var atAhead: Long = 0; private set
    @Volatile var atTooOld: Long = 0; private set
    /** 새로 받은 항목의 [TrafficEntry.requestsRejected] 합(순번으로 버린 항목은 세지 않는다) */
    @Volatile var requestsRejected: Long = 0; private set

    private val log = LoggerFactory.getLogger("observe")
    private var schemaReady = false
    private var groupReady = false
    private var lastUnacknowledgedCheck: Instant? = null
    private var lastLoggedFailure: String? = null                  // 같은 실패를 바퀴마다 되풀이해 로그에 남기지 않으려고
    private val dropped = EnumMap<TrafficDropReason, AtomicLong>(TrafficDropReason::class.java)
        .apply { TrafficDropReason.entries.forEach { put(it, AtomicLong()) } }
    private val cellDrops = EnumMap<TrafficCellDrop, AtomicLong>(TrafficCellDrop::class.java)
        .apply { TrafficCellDrop.entries.forEach { put(it, AtomicLong()) } }

    /** 버린 항목 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun droppedCounts(): Map<TrafficDropReason, Long> = dropped.mapValues { it.value.get() }

    /** 칸 · 요청별 줄만 버린 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun cellDropCounts(): Map<TrafficCellDrop, Long> = cellDrops.mapValues { it.value.get() }

    /** 한 바퀴. 처리한 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다 */
    fun runOnce(): Int {
        var step = "collect"
        return try {
            // 그룹을 먼저, 스키마는 DB 에 처음 닿았을 때 — 지표 수집과 같은 순서
            if (!groupReady) { source.ensureGroup(); groupReady = true }
            if (!schemaReady) { step = "schema"; sink.createSchema(); schemaReady = true; step = "collect"; log.info("observe:observe:traffic schema ready") }
            collect()
        } catch (error: Exception) {
            // 확인하지 않고 물러난다 — 다음 바퀴에 미확인분부터 다시 받고, 사라졌을 수 있는 그룹(Redis 재시작)도 다시 만든다
            lastUnacknowledgedCheck = null
            groupReady = false
            val failure = if (step == "schema") "schema ${error.javaClass.simpleName}" else error.javaClass.simpleName
            if (failure != lastLoggedFailure) log.error("observe:observe:traffic collect failed — will retry: {}", failure)
            lastLoggedFailure = failure
            lastFailure = failure
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
        lastLoggedFailure = null
        return handled
    }

    private fun storeAndAcknowledge(records: List<MetricStreamRecord>): Int {
        if (records.isEmpty()) return 0
        val receivedAt = clock()
        val accepted = ArrayList<Pair<String, TrafficEntry>>(records.size)
        val settledIds = ArrayList<String>()
        val droppedNow = ArrayList<Pair<String, TrafficDropReason>>()
        var otherKinds = 0
        for (record in records) {
            when (val parsed = TrafficEntryParser.parse(record, receivedAt)) {
                is TrafficEntryParse.Accepted -> accepted += record.id to parsed.entry
                is TrafficEntryParse.OtherKind -> { settledIds += record.id; otherKinds++ }
                is TrafficEntryParse.Dropped -> { settledIds += record.id; droppedNow += record.id to parsed.reason }
            }
        }
        // 버린 것은 확인에 성공한 뒤에 센다 — 확인이 실패하면 다시 받으므로 그때 한 번만 세게
        if (settledIds.isNotEmpty()) {
            source.acknowledge(settledIds)
            for ((id, reason) in droppedNow) {
                dropped.getValue(reason).incrementAndGet()
                log.warn("observe:observe:dropped traffic entry {}: {}", id.take(40), reason.wireName)
            }
            skippedOtherKind += otherKinds
        }
        // 항목마다 저장하고 확인한다 — 저장이 실패하면 예외로 물러나 그 항목과 뒤의 항목은 확인하지 않는다(다음 바퀴에 다시 받는다)
        for ((id, entry) in accepted) {
            val outcome = try {
                sink.store(entry)
            } catch (error: Exception) {
                lastStoreFailure = error.javaClass.simpleName
                lastStoreFailureAt = clock()
                throw error
            }
            source.acknowledge(listOf(id))
            count(entry, outcome)
        }
        return records.size
    }

    private fun count(entry: TrafficEntry, outcome: TrafficStoreOutcome) {
        if (outcome.duplicate) { duplicates++; return }
        storedTotal++
        cellDrops.getValue(TrafficCellDrop.SEGMENT_AHEAD).addAndGet(outcome.segmentsAhead.toLong())
        cellDrops.getValue(TrafficCellDrop.SEGMENT_TOO_OLD).addAndGet(outcome.segmentsTooOld.toLong())
        cellDrops.getValue(TrafficCellDrop.REQUEST_AHEAD).addAndGet(outcome.requestsAhead.toLong())
        cellDrops.getValue(TrafficCellDrop.REQUEST_TOO_OLD).addAndGet(outcome.requestsTooOld.toLong())
        if (TrafficCellAge.isAhead(entry.readAt, entry.receivedAt)) atAhead++
        if (TrafficCellAge.isTooOld(entry.readAt, entry.receivedAt)) atTooOld++
        requestsRejected += entry.requestsRejected
    }
}

/**
 * 통신 스트림의 Redis 소비자 그룹 — 지표의 [RedisMetricStreamSource] 를 스트림 이름만 달리해 그대로 쓴다(규약 4.1). 따로의 형으로 감싸는 것은
 * 빈의 형을 나누기 위해서다 — 같은 형의 빈이 둘이면 지표 수집이 어느 것을 받을지 정하지 못한다.
 */
class TrafficStreamSource(private val redis: RedisMetricStreamSource) : MetricStreamSource by redis {
    /** 그룹에 아직 전달되지 않은 밀린 건수 — 셀 수 없으면 null */
    fun lag(): Long? = redis.lag()
}
