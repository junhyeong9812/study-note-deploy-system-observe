package xyz.junproject.observe.traffic

import xyz.junproject.observe.metric.MetricStreamSource
import java.time.Duration
import java.time.Instant

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
 * 뼈대: 본문은 구현 워커가 채운다.
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

    /** 버린 항목 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun droppedCounts(): Map<TrafficDropReason, Long> = TODO("뼈대: 구현 워커가 채운다")

    /** 칸 · 요청별 줄만 버린 수 — 이유마다(모든 이유가 들고, 없으면 0) */
    fun cellDropCounts(): Map<TrafficCellDrop, Long> = TODO("뼈대: 구현 워커가 채운다")

    /** 한 바퀴. 처리한 항목 수를 돌려준다. 실패하면 예외 없이 0 을 돌려주고 이유를 남긴다 */
    fun runOnce(): Int = TODO("뼈대: 구현 워커가 채운다")
}
