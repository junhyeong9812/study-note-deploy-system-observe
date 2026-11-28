package xyz.junproject.observe.visit

import io.lettuce.core.XTrimArgs
import io.lettuce.core.api.async.RedisStreamAsyncCommands
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Instant

/** 정리 한 번(스트림의 안전망)의 결과(헬스용) — 실패면 [trimmed] 는 null 이고 [failure] 에 예외 종류 이름 */
data class VisitStreamTrimResult(val ranAt: Instant, val trimmed: Long?, val failure: String?)

/**
 * 방문 스트림에서 항목을 지우는 면(V2b) — 주소가 든 원문이 저장 뒤에 Redis 에 남지 않게. 방문 스트림에만 쓴다(로그 · 지표 스트림은 지우지 않는다 —
 * 함께 쓰는 [xyz.junproject.observe.collect.RedisLogStreamSource.acknowledge] 는 지우지 않는다).
 */
interface VisitStreamCleaner {
    /** `XDEL` — 지운 수(이미 없는 ID 는 세지 않는다 · 오류가 아니다). 실패는 예외 */
    fun delete(ids: List<String>): Long
    /**
     * `XTRIM <stream> MINID <cutoff 밀리초>`(근사 `~` 가 아니라 정확히) — 스트림 ID 의 밀리초가 [cutoff] 보다 앞선 항목을 지운다(같은 밀리초는 남긴다 —
     * DB 삭제의 `stream_ms < cutoff` 와 같은 경계). 미확인 목록에 든 항목도 잘라 낸다. 지운 수. 실패는 예외
     */
    fun trimOlderThan(cutoff: Instant): Long
}

/**
 * Redis 의 방문 스트림 하나. 자르기는 Lettuce 의 명령을 직접 쓴다 — Spring Data Redis 의 자르기는 개수(MAXLEN)만 받고, `MINID` 를 정확히(`=`) 주려면
 * `XTrimArgs.minId(…).exactTrimming()` 이 필요하다(근사 `~` 면 경계의 노드 하나가 통째로 남거나 남지 않아 기준 시각이 흔들린다). `MINID` 는 Redis 6.2 부터.
 */
class RedisVisitStreamCleaner(private val redis: StringRedisTemplate, private val stream: String) : VisitStreamCleaner {
    override fun delete(ids: List<String>): Long {
        if (ids.isEmpty()) return 0
        return redis.opsForStream<String, String>().delete(stream, *ids.map { RecordId.of(it) }.toTypedArray()) ?: 0
    }

    override fun trimOlderThan(cutoff: Instant): Long = redis.execute { connection ->
        @Suppress("UNCHECKED_CAST")
        val commands = connection.nativeConnection as RedisStreamAsyncCommands<ByteArray, ByteArray>
        // MINID <밀리초> — ID 가 `<밀리초>-0` 보다 작은 항목만 지운다(같은 밀리초는 남는다). 연결의 명령 제한 시간이 걸려 있다
        commands.xtrim(stream.toByteArray(), XTrimArgs.Builder.minId(cutoff.toEpochMilli().toString()).exactTrimming()).get()
    } ?: 0
}
