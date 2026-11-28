package xyz.junproject.observe.visit

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.log.StreamId
import java.sql.Statement

/**
 * 묶음 저장의 결과를 줄마다 가른다 — 드라이버가 줄마다 돌려준 수: 1 이상 = 새로 넣음 · 0 = 이미 있음 ·
 * `Statement.SUCCESS_NO_INFO`(결과 모름 — `reWriteBatchedInserts` 가 켜지면 모든 줄이 이것) = 넣은 것으로 치지 않고 [VisitInsertion.resultUnknown].
 * 드라이버 대신 정해진 수를 돌려주는 JdbcTemplate 으로 본다(실제 드라이버를 거친 것은 엔진 테스트).
 */
class VisitStoreInsertionTest {
    /** 묶음 저장에 [answers] 를 그대로 돌려주는 JdbcTemplate — 다른 호출은 쓰지 않는다 */
    private class AnsweringJdbcTemplate(private val answers: IntArray) : JdbcTemplate() {
        override fun batchUpdate(sql: String, batchArgs: List<Array<Any?>>, argTypes: IntArray): IntArray = answers
    }

    private fun store(answers: IntArray) = AnsweringJdbcTemplate(answers).let { VisitStore(it, it, it) }

    @Test
    fun `새로 넣음과 이미 있음과 결과 모름을 줄마다 가른다`() {
        val visits = listOf(VisitRecords.visit(1_000), VisitRecords.visit(2_000), VisitRecords.visit(3_000), VisitRecords.visit(4_000))
        val result = store(intArrayOf(1, 0, Statement.SUCCESS_NO_INFO, 1)).insertAll(visits)
        assertEquals(VisitInsertion(setOf(StreamId(1_000, 0), StreamId(4_000, 0)), setOf(StreamId(3_000, 0))), result,
            "결과 모름을 넣은 것으로 쳤거나 이미 있음과 섞었다")
    }

    @Test
    fun `모든 줄이 결과 모름이면 넣은 것은 하나도 없다`() {
        val visits = listOf(VisitRecords.visit(1_000), VisitRecords.visit(2_000))
        val result = store(intArrayOf(Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO)).insertAll(visits)
        assertEquals(emptySet<StreamId>(), result.inserted, "결과 모름을 새로 넣은 것으로 쳤다")
        assertEquals(setOf(StreamId(1_000, 0), StreamId(2_000, 0)), result.resultUnknown)
    }
}
