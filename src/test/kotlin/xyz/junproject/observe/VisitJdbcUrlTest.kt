package xyz.junproject.observe

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * 방문 풀 셋의 JDBC 주소 — 주어진 주소가 무엇이든 `logServerErrorDetail=false`(드라이버 예외 글에 서버 오류의 세부 · 넣으려던 값을 싣지 않는다)와
 * `reWriteBatchedInserts=false`(묶음 저장이 줄마다 결과를 돌려준다 — 켜지면 모든 줄이 "결과 모름"이 되어 저장한 수가 흔들린다)가 꼭 한 번씩 붙는다.
 * 이미 있는 값은 덧붙이지 않고 false 로 바꾸며, 다른 매개변수와 주소의 앞부분은 그대로 둔다.
 */
class VisitJdbcUrlTest {
    private val base = "jdbc:postgresql://postgres:5432/observe"

    /** 주소의 앞부분과 매개변수(나온 차례대로, 같은 이름이 둘이면 둘 다) */
    private fun split(url: String): Pair<String, List<Pair<String, String>>> {
        assertTrue(url.count { it == '?' } <= 1, "물음표가 둘 이상이다: $url")
        val head = url.substringBefore('?')
        val query = url.substringAfter('?', "")
        val parameters = if (query.isEmpty()) emptyList() else query.split('&').map { it.substringBefore('=') to it.substringAfter('=', "") }
        return head to parameters
    }

    private fun assertVisitUrl(given: String, expectedOthers: List<Pair<String, String>>) {
        val (head, parameters) = split(ObserveConfiguration.visitJdbcUrl(given))
        assertEquals(base, head, "주소의 앞부분이 바뀌었다")
        for (name in listOf("logServerErrorDetail", "reWriteBatchedInserts")) {
            assertEquals(listOf(name to "false"), parameters.filter { it.first == name }, "$given → $name 이 꼭 한 번 false 가 아니다: $parameters")
        }
        assertEquals(expectedOthers, parameters.filter { it.first !in setOf("logServerErrorDetail", "reWriteBatchedInserts") }, "다른 매개변수가 바뀌었다")
    }

    @Test
    fun `매개변수가 없는 주소에 둘 다 붙는다`() = assertVisitUrl(base, emptyList())

    @Test
    fun `이미 물음표가 있는 주소에는 앰퍼샌드로 이어 둘 다 붙는다`() =
        assertVisitUrl("$base?currentSchema=visit_http,public", listOf("currentSchema" to "visit_http,public"))

    @Test
    fun `주소에 reWriteBatchedInserts=true 가 있으면 false 로 바뀐다`() {
        assertVisitUrl("$base?reWriteBatchedInserts=true", emptyList())
        assertVisitUrl("$base?currentSchema=s&reWriteBatchedInserts=true&sslmode=disable", listOf("currentSchema" to "s", "sslmode" to "disable"))
    }

    @Test
    fun `둘 다 이미 true 로 있어도 둘 다 false 로 바뀌고 한 번씩만 있다`() =
        assertVisitUrl("$base?logServerErrorDetail=true&currentSchema=s&reWriteBatchedInserts=true", listOf("currentSchema" to "s"))

    @Test
    fun `이미 false 면 그대로 한 번만 있다`() =
        assertVisitUrl("$base?reWriteBatchedInserts=false&logServerErrorDetail=false", emptyList())
}
