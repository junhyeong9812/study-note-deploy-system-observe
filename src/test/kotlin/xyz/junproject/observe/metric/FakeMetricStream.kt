package xyz.junproject.observe.metric

import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/**
 * Redis 소비자 그룹 흉내(지표 스트림) — 새 항목은 한 번만 내주고 확인 전까지 미확인으로 남긴다.
 * [events] 에 확인한 ID 를 남겨 "저장한 뒤에 확인했는가"를 볼 수 있게 한다.
 */
class FakeMetricStream(private val events: MutableList<String> = mutableListOf()) : MetricStreamSource {
    private val undelivered = ArrayDeque<MetricStreamRecord>()
    val pending = LinkedHashMap<String, MetricStreamRecord>()
    val acknowledged = mutableListOf<String>()
    var groupExists = false
    var reachable = true

    fun add(vararg records: MetricStreamRecord) { undelivered.addAll(records) }

    private fun requireUsable() {
        if (!reachable) throw IllegalStateException("Redis 에 닿지 못함")
        if (!groupExists) throw IllegalStateException("NOGROUP No such key or consumer group")
    }

    override fun ensureGroup() {
        if (!reachable) throw IllegalStateException("Redis 에 닿지 못함")
        groupExists = true
    }

    override fun readUnacknowledged(count: Int): List<MetricStreamRecord> { requireUsable(); return pending.values.take(count) }

    override fun readNew(count: Int, block: Duration): List<MetricStreamRecord> {
        requireUsable()
        val delivered = generateSequence { undelivered.removeFirstOrNull() }.take(count).toList()
        delivered.forEach { pending[it.id] = it }
        return delivered
    }

    override fun acknowledge(ids: List<String>) {
        requireUsable()
        ids.forEach { events += "ack:$it" }
        ids.forEach { if (pending.remove(it) != null) acknowledged += it }
    }

    override fun unacknowledgedCount(): Long { requireUsable(); return pending.size.toLong() }
}

/** 메모리 저장소 — 같은 스트림 ID 는 한 번만 남긴다. [failing] 이면 저장이, [schemaFailing] 이면 스키마 생성이 실패한다 */
class FakeMetricSink(private val events: MutableList<String> = mutableListOf()) : MetricSink {
    val stored = LinkedHashMap<StreamId, ResourceSample>()
    var failing = false
    var schemaFailing = false
    var schemaCreations = 0

    override fun createSchema() {
        if (schemaFailing) throw IllegalStateException("permission denied for schema public")
        schemaCreations++
    }

    override fun insertAll(samples: List<ResourceSample>): Int {
        if (failing) { events += "insert-failed"; throw IllegalStateException("DB 에 닿지 못함") }
        samples.forEach { events += "insert:${it.id}" }
        return samples.count { stored.putIfAbsent(it.id, it) == null }
    }
}

/** 테스트용 스트림 항목 — 맞는 꼴의 자원 지표 하나 */
object MetricRecords {
    fun body(cpuPct: Double? = 3.4, containers: String = """[{"name": "front-app-green", "id": "0123456789ab", "project": "study-note-deploy-system-front", "cpu_pct": 0.7, "mem_used": 119000000, "restarts": 0, "state": "running"}]""") =
        """{"interval_ms": 30000, "host": {"cpu_pct": ${cpuPct ?: "null"}, "load1": 0.1, "mem_total": 16000000000, "mem_used": 2100000000,
           "disk_total": 500, "disk_used": 80, "net_rx_bps": 1200, "net_tx_bps": 3400, "io_read_bps": 0, "io_write_bps": 52000},
           "containers": $containers, "gpus": [], "missing": []}"""

    fun resource(id: String, measuredAt: Instant, server: String = "ci-agent-158", body: String = body()) =
        MetricStreamRecord(id, mapOf("kind" to "resource", "v" to "1", "server" to server, "at" to measuredAt.toEpochMilli().toString(), "body" to body))
}
