package xyz.junproject.observe.traffic

import xyz.junproject.observe.metric.MetricStreamRecord
import java.time.Instant

/**
 * 테스트용 통신 스트림 항목 — 규약 4.2 의 꼴 그대로(칸의 차례도 예시대로). 구간 묶음은 요청 줄의 상태 · 걸린 시간에서 규약 4.3 의 계산으로 만든다.
 * 분포의 경계는 규약 4.2 의 글자를 여기에 다시 적는다(구현의 상수를 쓰지 않는다).
 */
object TrafficRecords {
    const val GENERATION = "0123456789abcdef"
    const val OTHER_GENERATION = "fedcba9876543210"
    const val SERVER = "ci-agent-158"
    val UPPER_BOUNDS_MS = listOf(5L, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000, 20000, 30000, 45000, 60000, 90000, 120000, 180000)

    /** 걸린 시간이 드는 칸의 번호(0~19) — 위쪽 경계 이하인 첫 칸, 180초 초과는 19 */
    fun bucketIndex(durationMs: Long): Int = UPPER_BOUNDS_MS.indexOfFirst { durationMs <= it }.let { if (it < 0) 19 else it }

    /** 칸 시작(30초 경계로 내린다) */
    fun cellStart(at: Instant): Instant = Instant.ofEpochMilli(Math.floorDiv(at.toEpochMilli(), 30_000L) * 30_000L)

    /** 줄들의 상태 · 걸린 시간으로 만든 구간 묶음 하나(JSON). `request` 가 아니면 분포는 0 이고 `max_ms` 는 null */
    fun segment(
        bucketStart: Instant,
        statuses: List<Int>,
        durationsMs: List<Long> = statuses.map { 40L },
        service: String = "backend",
        caller: String = "front",
        trafficClass: String = "request",
        extra: String = "",
    ): String {
        require(statuses.size == durationsMs.size && statuses.isNotEmpty())
        val status = linkedMapOf("2xx" to 0L, "3xx" to 0L, "4xx" to 0L, "5xx" to 0L, "other" to 0L)
        for (code in statuses) {
            val key = when (code / 100) { 2 -> "2xx"; 3 -> "3xx"; 4 -> "4xx"; 5 -> "5xx"; else -> "other" }
            status[key] = status.getValue(key) + 1
        }
        val buckets = LongArray(20)
        if (trafficClass == "request") durationsMs.forEach { buckets[bucketIndex(it)]++ }
        val maxMs = if (trafficClass == "request") durationsMs.max().toString() else "null"
        return """{"bucket_start_ms": ${bucketStart.toEpochMilli()}, "service": "$service", "caller": "$caller", "class": "$trafficClass",
                   "count": ${statuses.size}, "status": {${status.entries.joinToString { "\"${it.key}\": ${it.value}" }}},
                   "buckets": [${buckets.joinToString()}], "max_ms": $maxMs$extra}"""
    }

    /** 요청별 줄 하나(JSON) */
    fun request(
        endAt: Instant,
        requestId: String = "req-3f9a0c1b2d4e",
        service: String = "backend",
        caller: String = "front",
        method: String = "GET",
        path: String = "/api/doc",
        pathTruncated: Boolean = false,
        status: Int = 200,
        durationMs: Long = 41,
        upstreamMs: Long? = 40,
        upstreamStatus: String? = "200",
        slot: String? = "blue",
        bytesSent: Long = 5120,
        extra: String = "",
    ): String =
        """{"request_id": "$requestId", "service": "$service", "caller": "$caller", "method": "$method", "path": ${quote(path)},
           "path_truncated": $pathTruncated, "status": $status, "end_ms": ${endAt.toEpochMilli()}, "duration_ms": $durationMs,
           "upstream_ms": ${upstreamMs ?: "null"}, "upstream_status": ${upstreamStatus?.let(::quote) ?: "null"}, "slot": ${slot?.let(::quote) ?: "null"},
           "bytes_sent": $bytesSent$extra}"""

    /** body 하나(JSON) — 칸은 생략하지 않는다(규약 4.3) */
    fun body(
        segments: List<String> = emptyList(),
        requests: List<String> = emptyList(),
        missing: List<String> = emptyList(),
        requestsDropped: Long = 0,
        extra: String = "",
    ): String =
        """{"sources": [{"container": "backend-nginx", "container_id": "3f9a0c1b2d4e",
                         "from": {"docker_ts": "2025-11-25T04:00:00.012345678Z", "skip": 0},
                         "to": {"docker_ts": "2025-11-25T04:00:30.020001000Z", "skip": 2}, "lines": 812, "unparsed": 0, "ignored": 0}],
           "missing": [${missing.joinToString { quote(it) }}],
           "segments": [${segments.joinToString()}],
           "requests": [${requests.joinToString()}],
           "requests_dropped": $requestsDropped,
           "dependencies": []$extra}"""

    fun fields(sequence: Long, at: Instant, body: String = body(), server: String = SERVER, generation: String = GENERATION): Map<String, String> =
        linkedMapOf("kind" to "traffic", "v" to "1", "server" to server, "generation" to generation, "seq" to sequence.toString(),
            "at" to at.toEpochMilli().toString(), "body" to body)

    fun record(id: String, sequence: Long, at: Instant, body: String = body(), server: String = SERVER, generation: String = GENERATION) =
        MetricStreamRecord(id, fields(sequence, at, body, server, generation))

    /** 받아들여진 항목 — 해석기를 거친다(꼴이 틀리면 테스트가 바로 실패한다) */
    fun entry(sequence: Long, at: Instant, receivedAt: Instant, body: String = body(), server: String = SERVER, generation: String = GENERATION): TrafficEntry {
        val parsed = TrafficEntryParser.parse(record("${receivedAt.toEpochMilli()}-$sequence", sequence, at, body, server, generation), receivedAt)
        check(parsed is TrafficEntryParse.Accepted) { "테스트 항목의 꼴이 틀렸다: $parsed" }
        return parsed.entry
    }

    private fun quote(text: String) = "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}

/** 메모리 저장소 — 같은 `(server, generation, seq)` 는 한 번만. [failing] 이면 저장이 실패한다(예외 문구에 항목의 경로를 싣는다 — 로그에 새지 않는지 보려고) */
class FakeTrafficSink(private val events: MutableList<String> = mutableListOf()) : TrafficSink {
    val stored = LinkedHashMap<Triple<String, String, Long>, TrafficEntry>()
    var failing: (TrafficEntry) -> Boolean = { false }
    var outcome: (TrafficEntry) -> TrafficStoreOutcome = { TrafficStoreOutcome(duplicate = false, segmentsStored = it.segments.size, requestsStored = it.requests.size) }
    var schemaCreations = 0

    override fun createSchema() { schemaCreations++ }

    override fun store(entry: TrafficEntry): TrafficStoreOutcome {
        if (failing(entry)) {
            events += "store-failed:${entry.sequence}"
            throw IllegalStateException("DB 에 닿지 못함 path=${entry.requests.firstOrNull()?.path}")
        }
        events += "store:${entry.sequence}"
        val key = Triple(entry.server, entry.generation, entry.sequence)
        if (stored.containsKey(key)) return TrafficStoreOutcome(duplicate = true)
        stored[key] = entry
        return outcome(entry)
    }
}
