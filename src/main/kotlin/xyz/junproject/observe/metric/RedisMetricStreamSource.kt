package xyz.junproject.observe.metric

import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

/**
 * 지표 스트림의 Redis 소비자 그룹 — 로그의 [xyz.junproject.observe.collect.RedisLogStreamSource] 와 같은 규칙
 * (소비자 이름 하나, 그룹은 스트림 처음부터, BUSYGROUP 은 지나간다). 항목의 칸은 전부 넘긴다.
 */
class RedisMetricStreamSource(
    private val redis: StringRedisTemplate,
    private val stream: String,
    private val group: String,
    private val consumerName: String = "observe",
) : MetricStreamSource {
    override fun ensureGroup(): Unit = TODO("not implemented")
    override fun readUnacknowledged(count: Int): List<MetricStreamRecord> = TODO("not implemented")
    override fun readNew(count: Int, block: Duration): List<MetricStreamRecord> = TODO("not implemented")
    override fun acknowledge(ids: List<String>): Unit = TODO("not implemented")
    override fun unacknowledgedCount(): Long = TODO("not implemented")

    /** 그룹에 아직 전달되지 않은 밀린 건수(`XINFO GROUPS` 의 lag) — 셀 수 없으면 null */
    fun lag(): Long? = TODO("not implemented")
}
