package xyz.junproject.observe.collect

import xyz.junproject.observe.log.LogEntry
import xyz.junproject.observe.log.LogSink
import xyz.junproject.observe.log.StreamId
import java.time.Duration

/**
 * Redis 소비자 그룹을 흉내 낸다 — 새 항목은 한 번만 내주고, 내준 뒤 확인(ack) 전까지는 미확인으로 남긴다.
 * [events] 에 일어난 순서를 남겨 "저장한 뒤에 확인했는가"를 볼 수 있게 한다.
 */
class FakeLogStream(private val events: MutableList<String> = mutableListOf()) : LogStreamSource {
    private val undelivered = ArrayDeque<StreamRecord>()
    val pending = LinkedHashMap<String, StreamRecord>()
    val acknowledged = mutableListOf<String>()
    var groupExists = false
    var reachable = true

    fun add(vararg records: StreamRecord) { undelivered.addAll(records) }

    /** Redis 가 디스크 저장 없이 재시작했다 — 스트림도 소비자 그룹도 사라진다 */
    fun restartWithoutPersistence() { undelivered.clear(); pending.clear(); groupExists = false }

    private fun requireUsable() {
        if (!reachable) throw IllegalStateException("Redis 에 닿지 못함")
        if (!groupExists) throw IllegalStateException("NOGROUP No such key or consumer group")
    }

    override fun ensureGroup() {
        if (!reachable) throw IllegalStateException("Redis 에 닿지 못함")
        groupExists = true
    }

    override fun readUnacknowledged(count: Int): List<StreamRecord> { requireUsable(); return pending.values.take(count) }

    override fun readNew(count: Int, block: Duration): List<StreamRecord> {
        requireUsable()
        val delivered = generateSequence { undelivered.removeFirstOrNull() }.take(count).toList()
        delivered.forEach { pending[it.id] = it }
        return delivered
    }

    override fun acknowledge(ids: List<String>) {
        requireUsable()
        events += "ack:${ids.size}"
        ids.forEach { if (pending.remove(it) != null) acknowledged += it }
    }

    override fun unacknowledgedCount(): Long { requireUsable(); return pending.size.toLong() }
}

/** 메모리 저장소 — 같은 스트림 ID 는 한 번만 남긴다(기본 키 흉내). [failing] 이면 저장이 예외로 실패한다. */
class FakeLogSink(private val events: MutableList<String> = mutableListOf()) : LogSink {
    val stored = LinkedHashMap<StreamId, LogEntry>()
    var failing = false

    override fun insertAll(entries: List<LogEntry>): Int {
        if (failing) { events += "insert-failed"; throw IllegalStateException("DB 에 닿지 못함") }
        events += "insert:${entries.size}"
        return entries.count { stored.putIfAbsent(it.id, it) == null }
    }
}
