package xyz.junproject.observe.collect

import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogLineParser
import xyz.junproject.observe.log.LogSink
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/** 스트림에서 받은 항목 하나(원문 그대로) */
data class StreamRecord(val id: String, val level: String?, val line: String?)

/** 로그 스트림을 소비자 그룹으로 읽는 면 — 실제 구현은 Redis */
interface LogStreamSource {
    /** 소비자 그룹이 없으면 만든다(스트림의 처음부터 읽게) */
    fun ensureGroup()
    /** 받았지만 아직 확인(ack)하지 못한 항목 — 지난번에 저장하다 죽었거나 실패한 것 */
    fun readUnacknowledged(count: Int): List<StreamRecord>
    /** 아직 아무에게도 가지 않은 새 항목. 없으면 [block] 만큼 기다린다 */
    fun readNew(count: Int, block: Duration): List<StreamRecord>
    fun acknowledge(ids: List<String>)
    /** 받았지만 확인하지 못한 항목 수 */
    fun unacknowledgedCount(): Long
}

/**
 * 수집 — 스트림에서 읽어 저장하고, **저장에 성공한 뒤에만** 확인(ack)한다.
 * 저장에 실패하면 확인하지 않으므로 그 항목들은 미확인으로 남아 다음에 다시 받는다(잃지 않는다).
 * 같은 항목을 다시 받아 저장해도 기본 키가 한 번만 남긴다 — 그래서 두 인스턴스가 함께 돌아도 안전하다.
 */
class LogCollector(
    private val source: LogStreamSource,
    private val sink: LogSink,
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

    @Volatile var lastLoopAt: Instant? = null; private set          // 마지막으로 한 바퀴를 끝까지 돈 시각(읽을 것이 없었어도)
    // 마지막 바퀴가 실패했으면 그 예외의 종류 이름(스키마 단계면 앞에 "schema "), 성공했으면 null.
    // 인증 없는 헬스에 그대로 나가므로 예외 문구(접속 주소·SQL)는 담지 않는다 — 문구는 서버 로그에만.
    @Volatile var lastFailure: String? = null; private set
    @Volatile var storedTotal: Long = 0; private set
    /** 뜬 뒤 처음으로 따라잡은 시각(새 항목을 읽었는데 하나도 없던 첫 바퀴) — 뼈대(03): 아직 채우지 않는다 */
    @Volatile var caughtUpAt: Instant? = null; private set

    /** 한 바퀴. 처리한(저장하고 확인한) 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다. */
    fun runOnce(): Int {
        var step = "collect"
        return try {
            // 그룹을 먼저 — 그룹이 없으면 헬스의 미확인 건수 조회가 NOGROUP 으로 실패해 스키마 실패가 "Redis 불통"으로 가려진다
            if (!groupReady) { source.ensureGroup(); groupReady = true }
            // 스키마는 DB 에 처음 닿았을 때 만든다 — DB 가 늦게 떠도 이 바퀴가 실패로 남기고 다음 바퀴에 다시 시도한다
            if (!schemaReady) { step = "schema"; sink.createSchema(); schemaReady = true; step = "collect"; log.info("observe:observe:schema ready") }
            collect()
        } catch (error: Exception) {
            // 저장 실패든 Redis 불통이든 확인하지 않고 물러난다 — 미확인분 재확인을 다음 바퀴에 다시 하게 한다.
            // 그룹도 다시 확인한다 — Redis 는 디스크에 저장하지 않아 재시작하면 스트림과 그룹이 함께 사라지는데,
            // 그룹을 다시 만들지 않으면 NOGROUP 만 되풀이하며 수집이 영영 멈춘다.
            lastUnacknowledgedCheck = null
            groupReady = false
            val kind = error.javaClass.simpleName
            val detail = "$step $kind: ${error.message?.take(200)}"
            if (detail != lastFailureDetail) log.error("observe:observe:collect failed — will retry: {}", detail)
            lastFailureDetail = detail
            lastFailure = if (step == "schema") "schema $kind" else kind
            0
        }
    }

    private fun collect(): Int {
        val now = clock()
        var handled = 0
        // 미확인분부터 — 기동 직후, 그리고 주기적으로(다른 인스턴스가 저장하다 죽으며 남긴 것을 거둔다)
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

    private fun storeAndAcknowledge(records: List<StreamRecord>): Int {
        if (records.isEmpty()) return 0
        val entries = ArrayList<LogEntry>(records.size)
        var trimmed = 0
        for (record in records) {
            val id = StreamId.parse(record.id)
            if (id == null) { log.warn("observe:observe:skipped entry with unreadable id {}", record.id.take(40)); continue }
            // 미확인인 채로 스트림 상한에 밀려난 항목은 ID 만 오고 필드가 비어 있다 — 로그 줄이 아니므로 빈 행을 만들지 않고 확인만 한다
            if (record.level == null && record.line == null) { trimmed++; continue }
            entries += LogLineParser.parse(id, record.level, record.line)
        }
        if (trimmed > 0) log.warn("observe:observe:{} unacknowledged entries were already trimmed from the stream — lost", trimmed)
        sink.insertAll(entries)                       // 실패하면 예외 → 아래 확인을 하지 않는다
        source.acknowledge(records.map { it.id })     // 읽을 수 없는 ID 의 항목도 함께 확인한다(영원히 미확인으로 남지 않게)
        storedTotal += entries.size
        return records.size
    }
}
