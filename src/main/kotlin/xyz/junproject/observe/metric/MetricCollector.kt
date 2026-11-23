package xyz.junproject.observe.metric

import java.time.Duration
import java.time.Instant

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
    /** 마지막으로 한 바퀴를 끝까지 돈 시각(읽을 것이 없었어도) */
    @Volatile var lastLoopAt: Instant? = null; private set
    /** 마지막 바퀴가 실패했으면 예외 종류 이름(스키마 단계면 앞에 "schema "), 성공했으면 null — 문구는 서버 로그에만 */
    @Volatile var lastFailure: String? = null; private set
    /** 이 프로세스가 뜬 뒤 저장한 항목 수 */
    @Volatile var storedTotal: Long = 0; private set
    /** 이 프로세스가 뜬 뒤 다른 종류라서 건너뛴 항목 수 */
    @Volatile var skippedOtherKind: Long = 0; private set

    /** 이 프로세스가 뜬 뒤 버린 항목 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun droppedCounts(): Map<MetricDropReason, Long> = TODO("not implemented")

    /** 한 바퀴. 처리한(저장하거나 버리거나 건너뛰고 확인한) 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다. */
    fun runOnce(): Int = TODO("not implemented")
}
