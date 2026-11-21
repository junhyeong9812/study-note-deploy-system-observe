package xyz.junproject.observe.collect

import org.springframework.data.redis.RedisSystemException
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

/**
 * Redis Stream 소비자 그룹.
 * 소비자 이름은 인스턴스마다 다르게 하지 않고 하나로 둔다 — 무중단 배포로 두 인스턴스가 겹쳐도 새 항목은 한쪽에만 가고,
 * 한쪽이 저장하다 죽으며 남긴 미확인분은 같은 이름의 다른 인스턴스가 그대로 다시 받는다(따로 넘겨받는 절차가 없다).
 */
class RedisLogStreamSource(
    private val redis: StringRedisTemplate,
    private val stream: String,
    private val group: String,
    private val consumerName: String = "observe",
) : LogStreamSource {
    private val operations = redis.opsForStream<String, String>()
    private val consumer = Consumer.from(group, consumerName)

    override fun ensureGroup() {
        try {
            // 스트림의 처음부터 — 관측 서버가 처음 뜰 때 남아 있던 로그도 받는다. 스트림이 없으면 함께 만든다.
            operations.createGroup(stream, ReadOffset.from("0"), group)
        } catch (error: RedisSystemException) {
            if (generateSequence<Throwable>(error) { it.cause }.none { it.message?.contains("BUSYGROUP") == true }) throw error
        }
    }

    override fun readUnacknowledged(count: Int): List<StreamRecord> =
        convert(operations.read(consumer, StreamReadOptions.empty().count(count.toLong()), StreamOffset.create(stream, ReadOffset.from("0"))))

    override fun readNew(count: Int, block: Duration): List<StreamRecord> =
        convert(operations.read(consumer, StreamReadOptions.empty().count(count.toLong()).block(block), StreamOffset.create(stream, ReadOffset.lastConsumed())))

    override fun acknowledge(ids: List<String>) {
        if (ids.isNotEmpty()) operations.acknowledge(stream, group, *ids.map { RecordId.of(it) }.toTypedArray())
    }

    override fun unacknowledgedCount(): Long = operations.pending(stream, group)?.totalPendingMessages ?: 0

    private fun convert(records: List<MapRecord<String, String, String>>?): List<StreamRecord> =
        records.orEmpty().map { StreamRecord(it.id.value, it.value["level"], it.value["line"]) }
}
