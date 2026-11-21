package xyz.junproject.observe.log

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

/** 명세 ②: 한 줄 `<요청ID>:<서비스>:<내용>` 을 나누고, 나눌 수 없는 줄도 버리지 않는다. 시각은 스트림 ID 의 밀리초. */
class LogLineParserTest {
    private val id = StreamId(1_763_700_000_123, 4)

    @Test
    fun `정상 줄은 요청ID·서비스·내용으로 나뉜다`() {
        val entry = LogLineParser.parse(id, "info", "req-1a2b3c4d5e6f:backend:문서 열림 id=42")
        assertEquals("req-1a2b3c4d5e6f", entry.requestId)
        assertEquals("backend", entry.service)
        assertEquals("문서 열림 id=42", entry.message)
        assertEquals("info", entry.level)
        assertEquals(id, entry.id)
    }

    @Test
    fun `네 서비스의 이름이 모두 서비스로 나뉜다`() {
        for (service in listOf("front", "backend", "llm-wrapper", "ci-master", "ci-agent")) {
            assertEquals(service, LogLineParser.parse(id, "info", "req-9:$service:hello").service)
        }
    }

    @Test
    fun `내용에 콜론이 있어도 앞의 두 콜론에서만 나눈다`() {
        val entry = LogLineParser.parse(id, "error", "req-7:front:GET /docs?id=1 status:500 at 12:30:01 — error: timeout")
        assertEquals("req-7", entry.requestId)
        assertEquals("front", entry.service)
        assertEquals("GET /docs?id=1 status:500 at 12:30:01 — error: timeout", entry.message)
        assertEquals("error", entry.level)
    }

    @Test
    fun `내용에 줄바꿈이 있어도 한 항목으로 나뉘고 줄바꿈이 남는다`() {
        val entry = LogLineParser.parse(id, "error", "req-7:backend:예외 발생\n\tat a.b.C(C.kt:10)\n\tat a.b.D(D.kt:20)")
        assertEquals("backend", entry.service)
        assertEquals("예외 발생\n\tat a.b.C(C.kt:10)\n\tat a.b.D(D.kt:20)", entry.message)
    }

    @Test
    fun `나눌 수 없는 줄은 서비스 unknown 에 원문 그대로 남는다`() {
        for (line in listOf("콜론이 하나도 없는 줄", "콜론이:하나뿐", "공백 있는 요청:backend:내용")) {
            val entry = LogLineParser.parse(id, "warning", line)
            assertEquals(LogLineParser.UNKNOWN_SERVICE, entry.service, line)
            assertEquals(line, entry.message, line)
            assertEquals("warning", entry.level)
        }
    }

    @Test
    fun `빈 줄과 line 필드가 없는 항목도 버리지 않는다`() {
        for (line in listOf("", null)) {
            val entry = LogLineParser.parse(id, "info", line)
            assertEquals(LogLineParser.UNKNOWN_SERVICE, entry.service)
            assertEquals("", entry.message)
            assertEquals(id, entry.id)
        }
    }

    @Test
    fun `NUL 문자는 지워져 저장할 수 있는 글이 된다`() {
        val entry = LogLineParser.parse(id, "info", "req-1:backend:앞\u0000뒤")
        assertEquals("backend", entry.service)
        assertEquals("앞뒤", entry.message)
        assertFalse(entry.message.contains('\u0000'))
    }

    @Test
    fun `아주 긴 줄도 버려지지 않고 앞부분이 남는다`() {
        val content = "가".repeat(200_000) + "끝"
        val entry = LogLineParser.parse(id, "info", "req-1:llm-wrapper:$content")
        assertEquals("llm-wrapper", entry.service)
        assertTrue(entry.message.isNotEmpty())
        assertTrue(content.startsWith(entry.message), "앞부분이 원문과 같아야 한다")
    }

    @Test
    fun `레벨은 발신 쪽이 보낸 info·warning·error 를 그대로 쓴다`() {
        for (level in listOf("info", "warning", "error")) assertEquals(level, LogLineParser.parse(id, level, "req-1:front:x").level)
    }
}

/** 명세 ②: 로그 시각 = 스트림 ID 의 밀리초 */
class StreamIdTest {
    @Test
    fun `스트림 ID 의 밀리초가 로그 시각이 된다`() {
        val id = StreamId.parse("1763700000123-7")!!
        assertEquals(Instant.parse("2025-11-21T04:40:00.123Z"), id.at)
        assertEquals(1_763_700_000_123, id.millis)
        assertEquals(7, id.sequence)
        assertEquals("1763700000123-7", id.toString())
    }

    @Test
    fun `같은 밀리초 안에서는 순번이 순서를 정한다`() {
        val earlier = StreamId.parse("1763700000123-2")!!
        val later = StreamId.parse("1763700000123-10")!!
        assertTrue(earlier < later)
        assertTrue(StreamId.parse("1763700000122-99")!! < earlier)
    }

    @Test
    fun `스트림 ID 꼴이 아니면 읽지 않는다`() {
        for (text in listOf("", "abc", "1763700000123", "1763700000123-", "-1", "1-2-3", "1763700000123-x", " 1-2")) {
            assertNull(StreamId.parse(text), text)
        }
    }
}
