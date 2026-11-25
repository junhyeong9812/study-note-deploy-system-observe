package xyz.junproject.observe.metric

import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import org.springframework.data.redis.core.StringRedisTemplate
import xyz.junproject.observe.collect.RedisLogStreamSource
import java.time.Duration

/**
 * 지표 스트림의 Redis 소비자 그룹 — 로그의 [xyz.junproject.observe.collect.RedisLogStreamSource] 와 같은 규칙
 * (소비자 이름 하나, 그룹은 스트림 처음부터, BUSYGROUP 은 지나간다). 항목의 칸은 전부 넘긴다.
 * 그룹 · 확인 · 미확인 건수 · 밀린 건수는 칸과 무관해 로그의 것을 그대로 쓰고, 읽기만 칸 전부를 넘기게 따로 둔다.
 */
class RedisMetricStreamSource(
    private val redis: StringRedisTemplate,
    private val stream: String,
    private val group: String,
    private val consumerName: String = "observe",
) : MetricStreamSource {
    private val operations = redis.opsForStream<String, String>()
    private val consumer = Consumer.from(group, consumerName)
    private val groupCommands = RedisLogStreamSource(redis, stream, group, consumerName)

    override fun ensureGroup() = groupCommands.ensureGroup()

    override fun readUnacknowledged(count: Int): List<MetricStreamRecord> =
        convert(operations.read(consumer, StreamReadOptions.empty().count(count.toLong()), StreamOffset.create(stream, ReadOffset.from("0"))))

    override fun readNew(count: Int, block: Duration): List<MetricStreamRecord> =
        convert(operations.read(consumer, StreamReadOptions.empty().count(count.toLong()).block(block), StreamOffset.create(stream, ReadOffset.lastConsumed())))

    override fun acknowledge(ids: List<String>) = groupCommands.acknowledge(ids)

    override fun unacknowledgedCount(): Long = groupCommands.unacknowledgedCount()

    /** 그룹에 아직 전달되지 않은 밀린 건수(`XINFO GROUPS` 의 lag) — 셀 수 없으면 null */
    fun lag(): Long? = groupCommands.lag()

    /** 상한에 밀려난 미확인 항목은 칸이 비어 온다 — 빈 칸 그대로 넘겨 해석이 `trimmed` 로 센다 */
    private fun convert(records: List<MapRecord<String, String, String>>?): List<MetricStreamRecord> =
        records.orEmpty().map { MetricStreamRecord(it.id.value, it.value.orEmpty()) }
}
