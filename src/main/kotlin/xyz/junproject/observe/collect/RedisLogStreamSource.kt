package xyz.junproject.observe.collect

import io.lettuce.core.api.async.RedisStreamAsyncCommands
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

    /**
     * 그룹에 아직 전달되지 않은 밀린 건수(`XINFO GROUPS` 의 `lag`, Redis 7 부터). 미확인 건수는 "받았지만 확인 못 한" 것만이라
     * 수집이 밀려 아직 받지도 못한 양은 이것으로만 보인다. Spring Data Redis 의 XInfoGroup 이 lag 를 주지 않고, 스프링의 저수준
     * `execute` 는 겹친 목록 응답을 읽지 못해(바이트 한 덩이로만 받는다) Lettuce 의 명령을 직접 쓴다 — 응답은 겹친 목록 그대로 온다.
     * Redis 가 셀 수 없다고 하거나(nil — 스트림 중간이 지워졌을 때 등) 그룹이 없으면 null. 스트림이 없으면 예외(ERR no such key).
     */
    fun lag(): Long? {
        val groups = redis.execute { connection ->
            @Suppress("UNCHECKED_CAST")
            val commands = connection.nativeConnection as RedisStreamAsyncCommands<ByteArray, ByteArray>
            commands.xinfoGroups(stream.toByteArray()).get()   // 연결의 명령 제한 시간(application.yml 10초)이 걸려 있다
        } ?: return null
        for (fields in groups) {
            val pairs = (fields as? List<*>)?.chunked(2)?.associate { text(it[0]) to it.getOrNull(1) } ?: continue
            if (text(pairs["name"]) == group) return (pairs["lag"] as? Number)?.toLong()
        }
        return null
    }

    private fun text(value: Any?): String? = when (value) {
        is ByteArray -> String(value)
        null -> null
        else -> value.toString()
    }

    private fun convert(records: List<MapRecord<String, String, String>>?): List<StreamRecord> =
        records.orEmpty().map { StreamRecord(it.id.value, it.value["level"], it.value["line"]) }
}
