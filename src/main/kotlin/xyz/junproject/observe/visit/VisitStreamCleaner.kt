package xyz.junproject.observe.visit

import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Instant

/** 정리 한 번(스트림의 안전망)의 결과(헬스용) — 실패면 [trimmed] 는 null 이고 [failure] 에 예외 종류 이름 */
data class VisitStreamTrimResult(val ranAt: Instant, val trimmed: Long?, val failure: String?)

/**
 * 방문 스트림에서 항목을 지우는 면(V2b) — 주소가 든 원문이 저장 뒤에 Redis 에 남지 않게. 방문 스트림에만 쓴다(로그 · 지표 스트림은 지우지 않는다).
 * 뼈대: 본문은 구현 워커가 채운다.
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

/** Redis 의 방문 스트림 하나 — 뼈대: 구현 워커가 채운다 */
class RedisVisitStreamCleaner(private val redis: StringRedisTemplate, private val stream: String) : VisitStreamCleaner {
    override fun delete(ids: List<String>): Long = TODO("뼈대: 구현 워커가 채운다")
    override fun trimOlderThan(cutoff: Instant): Long = TODO("뼈대: 구현 워커가 채운다")
}
