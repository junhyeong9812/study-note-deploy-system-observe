package xyz.junproject.observe.traffic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.metric.MetricStreamRecord
import java.time.Duration
import java.time.Instant

/**
 * 규약 4.2 · 4.3 · 4.5 · 7.2 — 통신 항목의 해석(순수). 받아들인 항목의 값, 항목 전체를 버리는 이유(표), 모르는 칸은 무시,
 * `at` 과 칸의 시각으로는 해석이 버리지 않는다(그것은 저장이 순번 다음에 한다).
 */
class TrafficEntryParserTest {
    private val receivedAt = Instant.parse("2026-11-25T04:00:40Z")
    private val cell = Instant.parse("2026-11-25T04:00:00Z")

    private fun parse(fields: Map<String, String>, id: String = "1764043240000-0") = TrafficEntryParser.parse(MetricStreamRecord(id, fields), receivedAt)
    private fun fields(body: String = TrafficRecords.body(), sequence: Long = 7, at: Instant = receivedAt.minusSeconds(1)) = TrafficRecords.fields(sequence, at, body)
    private fun accepted(fields: Map<String, String>): TrafficEntry {
        val parsed = parse(fields)
        assertTrue(parsed is TrafficEntryParse.Accepted, "받아야 하는데: $parsed")
        return (parsed as TrafficEntryParse.Accepted).entry
    }
    private fun assertDropped(reason: TrafficDropReason, fields: Map<String, String>, label: String) =
        assertEquals(TrafficEntryParse.Dropped(reason), parse(fields), label)

    @Test
    fun `맞는 꼴의 항목은 칸마다의 값 그대로 받아들인다`() {
        val body = TrafficRecords.body(
            segments = listOf(
                TrafficRecords.segment(cell, statuses = listOf(200, 200, 304, 404, 502), durationsMs = listOf(3, 5, 6, 250, 181_000)),   // 5 는 첫 칸(이하) · 6 은 둘째 칸
                TrafficRecords.segment(cell, statuses = listOf(200, 200), service = "observe", caller = "alert", trafficClass = "health"),
            ),
            requests = listOf(
                TrafficRecords.request(cell.plusMillis(1234), requestId = "req-a_1", path = "/search;q=x", pathTruncated = true, status = 499,
                    durationMs = 1005, upstreamMs = null, upstreamStatus = null, slot = null, bytesSent = 0),
            ),
            missing = listOf("backend-nginx:rotated", "ci-cd-agent:unreadable"),
            requestsDropped = 12,
        )
        val entry = accepted(fields(body, sequence = 42, at = receivedAt.minusSeconds(3)))
        assertEquals(TrafficRecords.SERVER, entry.server)
        assertEquals(TrafficRecords.GENERATION, entry.generation)
        assertEquals(42L, entry.sequence)
        assertEquals(receivedAt.minusSeconds(3), entry.readAt)
        assertEquals(receivedAt, entry.receivedAt)
        assertEquals(12L, entry.requestsDropped)
        assertEquals(listOf("backend-nginx:rotated", "ci-cd-agent:unreadable"), entry.missing)
        assertEquals(
            TrafficSegment(cell, "backend", "front", TrafficClass.REQUEST, TrafficTotals(5, TrafficStatusCounts(2, 1, 1, 1, 0),
                listOf(2L, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1), 181_000)),
            entry.segments[0],
        )
        assertEquals(
            TrafficSegment(cell, "observe", "alert", TrafficClass.HEALTH, TrafficTotals(2, TrafficStatusCounts(2, 0, 0, 0, 0), List(20) { 0L }, null)),
            entry.segments[1],
        )
        assertEquals(
            TrafficRequest("req-a_1", "backend", "front", "GET", "/search;q=x", true, 499, cell.plusMillis(1234), 1005, null, null, null, 0),
            entry.requests.single(),
        )
    }

    @Test
    fun `빈 항목도 받아들인다 — 에이전트가 살아 있다는 신호`() {
        val entry = accepted(fields())
        assertEquals(emptyList<TrafficSegment>(), entry.segments)
        assertEquals(emptyList<TrafficRequest>(), entry.requests)
        assertEquals(0L, entry.requestsDropped)
    }

    @Test
    fun `body 는 32768 바이트까지 받고 1 바이트 넘으면 body_too_large`() {
        val base = TrafficRecords.body()
        val padding = 32 * 1024 - base.toByteArray().size - ", \"padding\": \"\"".length
        val exactly = TrafficRecords.body(extra = ", \"padding\": \"${"가".repeat(padding / 3)}${"a".repeat(padding % 3)}\"")
        assertEquals(32 * 1024, exactly.toByteArray(Charsets.UTF_8).size)
        accepted(fields(exactly))
        val over = TrafficRecords.body(extra = ", \"padding\": \"${"가".repeat(padding / 3)}${"a".repeat(padding % 3 + 1)}\"")
        assertDropped(TrafficDropReason.BODY_TOO_LARGE, fields(over), "32769 바이트")
    }

    @Test
    fun `스트림 칸이 틀리면 항목 전체를 버리고 이유를 정한 이름으로 낸다`() {
        val good = fields()
        val cases = listOf(
            Triple(TrafficDropReason.MISSING_FIELD, good - "v", "v 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "server", "server 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "generation", "generation 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "seq", "seq 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "at", "at 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "body", "body 없음"),
            Triple(TrafficDropReason.MISSING_FIELD, good + ("seq" to ""), "seq 빈 글자"),
            Triple(TrafficDropReason.MISSING_FIELD, good - "kind", "kind 없음"),
            Triple(TrafficDropReason.UNSUPPORTED_VERSION, good + ("v" to "2"), "모르는 판"),
            Triple(TrafficDropReason.UNSUPPORTED_VERSION, good + ("v" to "1.0"), "판의 글자가 다름"),
            Triple(TrafficDropReason.INVALID_SERVER, good + ("server" to "BAD SERVER"), "서버 이름 꼴"),
            Triple(TrafficDropReason.INVALID_GENERATION, good + ("generation" to "0123456789abcde"), "세대 15자"),
            Triple(TrafficDropReason.INVALID_GENERATION, good + ("generation" to "0123456789abcdef0"), "세대 17자"),
            Triple(TrafficDropReason.INVALID_GENERATION, good + ("generation" to "0123456789ABCDEF"), "세대 대문자"),
            Triple(TrafficDropReason.INVALID_GENERATION, good + ("generation" to "0123456789abcdeg"), "세대 16진수 밖"),
            Triple(TrafficDropReason.INVALID_SEQ, good + ("seq" to "0"), "순번 0"),
            Triple(TrafficDropReason.INVALID_SEQ, good + ("seq" to "-1"), "순번 음수"),
            Triple(TrafficDropReason.INVALID_SEQ, good + ("seq" to "01"), "순번 앞자리 0"),
            Triple(TrafficDropReason.INVALID_SEQ, good + ("seq" to "1.5"), "순번 소수"),
            Triple(TrafficDropReason.INVALID_SEQ, good + ("seq" to "seven"), "순번 글자"),
            Triple(TrafficDropReason.INVALID_AT, good + ("at" to "0"), "at 0"),
            Triple(TrafficDropReason.INVALID_AT, good + ("at" to "yesterday"), "at 글자"),
            Triple(TrafficDropReason.BODY_NOT_JSON, good + ("body" to "{"), "JSON 아님"),
            Triple(TrafficDropReason.BODY_NOT_JSON, good + ("body" to "${TrafficRecords.body()} {}"), "객체 뒤에 더 붙음"),
        )
        for ((reason, given, label) in cases) assertDropped(reason, given, label)
        assertEquals(TrafficEntryParse.Dropped(TrafficDropReason.INVALID_ID), parse(good, id = "nope"), "스트림 ID 꼴")
        assertEquals(TrafficEntryParse.Dropped(TrafficDropReason.TRIMMED), parse(emptyMap()), "칸이 하나도 없음")
    }

    @Test
    fun `body 의 꼴이 규약과 다르면 invalid_body 로 버린다`() {
        val request = TrafficRecords.segment(cell, statuses = listOf(200, 500), durationsMs = listOf(40, 60))
        fun segmentWith(replace: Pair<String, String>) = request.replace(replace.first, replace.second).also { assertNotEquals(request, it, "바꿀 글자를 못 찾음: ${replace.first}") }
        val bodies = listOf(
            "[]" to "객체가 아님",
            TrafficRecords.body().replace("\"requests_dropped\": 0,", "") to "requests_dropped 칸 빠짐",
            TrafficRecords.body().replace("\"dependencies\": []", "\"dependencies\": {}") to "dependencies 가 배열이 아님",
            TrafficRecords.body().replace("\"segments\": []", "\"segments\": {}") to "segments 가 배열이 아님",
            TrafficRecords.body(missing = listOf("backend-nginx")) to "missing 이 <컨테이너>:<이유> 꼴이 아님",
            TrafficRecords.body(segments = listOf(segmentWith("\"buckets\": [0, " to "\"buckets\": ["))) to "분포가 19칸",
            TrafficRecords.body(segments = listOf(segmentWith("\"bucket_start_ms\": ${cell.toEpochMilli()}" to "\"bucket_start_ms\": ${cell.toEpochMilli() + 1}"))) to "칸 시작이 30초 경계가 아님",
            TrafficRecords.body(segments = listOf(segmentWith("\"class\": \"request\"" to "\"class\": \"visit\""))) to "모르는 갈래",
            TrafficRecords.body(segments = listOf(segmentWith("\"other\": 0" to "\"other\": 1"))) to "상태의 합이 count 와 다름",
            TrafficRecords.body(segments = listOf(segmentWith(", \"other\": 0" to ""))) to "상태 열쇠 빠짐",
            TrafficRecords.body(segments = listOf(segmentWith("\"count\": 2" to "\"count\": 2.5"))) to "count 가 정수가 아님",
            TrafficRecords.body(segments = listOf(segmentWith("\"max_ms\": 60" to "\"max_ms\": null"))) to "request 의 max_ms 가 null",
            TrafficRecords.body(segments = listOf(segmentWith("\"service\": \"backend\"" to "\"service\": \"Backend Server\""))) to "서비스 이름 꼴",
            TrafficRecords.body(segments = listOf(request, request)) to "같은 열쇠의 묶음이 둘",
            TrafficRecords.body(segments = listOf(TrafficRecords.segment(cell, listOf(200), trafficClass = "health").replace("\"max_ms\": null", "\"max_ms\": 3"))) to "health 의 max_ms 가 null 이 아님",
            TrafficRecords.body(segments = listOf(TrafficRecords.segment(cell, listOf(200), trafficClass = "stream").replace("\"buckets\": [0", "\"buckets\": [1"))) to "stream 의 분포가 0 이 아님",
            TrafficRecords.body(segments = listOf(segmentWith("\"buckets\": [0, " to "\"buckets\": [1, "))) to "request 의 분포 합이 count 와 다름",
            TrafficRecords.body(requests = listOf(TrafficRecords.request(cell, path = "/" + "a".repeat(1024)))) to "경로가 1024자를 넘는다",
            TrafficRecords.body(requests = listOf(TrafficRecords.request(cell, requestId = "req:1"))) to "요청 ID 꼴",
            TrafficRecords.body(requests = listOf(TrafficRecords.request(cell).replace("\"duration_ms\": 41", "\"duration_ms\": -1"))) to "걸린 시간이 음수",
            TrafficRecords.body(requests = listOf(TrafficRecords.request(cell).replace("\"status\": 200", "\"status\": \"200\""))) to "상태가 글자",
            TrafficRecords.body(requests = List(101) { TrafficRecords.request(cell.plusMillis(it.toLong())) }) to "요청별 줄이 100 줄을 넘는다",
        )
        for ((body, label) in bodies) assertDropped(TrafficDropReason.INVALID_BODY, fields(body), label)
        // 경계의 안쪽 — 1024자 경로와 100 줄은 받는다
        accepted(fields(TrafficRecords.body(requests = listOf(TrafficRecords.request(cell, path = "/" + "a".repeat(1023)))))).let { assertEquals(1024, it.requests.single().path.length) }
        accepted(fields(TrafficRecords.body(requests = List(100) { TrafficRecords.request(cell.plusMillis(it.toLong())) }))).let { assertEquals(100, it.requests.size) }
    }

    @Test
    fun `모르는 칸은 무시하고 받는다 — 스트림 칸 · body · 묶음 · 요청별 줄 모두`() {
        val body = TrafficRecords.body(
            segments = listOf(TrafficRecords.segment(cell, listOf(200), extra = ", \"p99_hint\": 3")),
            requests = listOf(TrafficRecords.request(cell, extra = ", \"remote_addr\": \"203.0.113.9\", \"query\": \"q=MARKER-query\"")),
            extra = ", \"future_field\": {\"x\": 1}",
        )
        val entry = accepted(fields(body) + ("trace" to "anything"))
        assertEquals(1, entry.segments.size)
        assertEquals("/api/doc", entry.requests.single().path)
        assertFalse(entry.toString().contains("203.0.113.9"), "모르는 칸의 값이 해석 결과에 실렸다")
        assertFalse(entry.toString().contains("MARKER"), "모르는 칸의 값이 해석 결과에 실렸다")
    }

    @Test
    fun `요청 줄을 해석하지 못한 줄(method · path 가 빈 글자)만으로 된 request 묶음도 받는다 — 요청별 줄은 없다`() {
        val body = TrafficRecords.body(segments = listOf(TrafficRecords.segment(cell, statuses = listOf(400, 414, 505), durationsMs = listOf(0, 1, 2))))
        val entry = accepted(fields(body))
        assertEquals(TrafficTotals(3, TrafficStatusCounts(0, 0, 2, 1, 0), List(20) { if (it == 0) 3L else 0L }, 2), entry.segments.single().totals)
        assertEquals(emptyList<TrafficRequest>(), entry.requests)
    }

    @Test
    fun `경로에 쿼리나 주소 꼴이 든 요청별 줄은 그 줄만 버리고 세며 묶음과 다른 줄은 그대로 받는다`() {
        val body = TrafficRecords.body(
            segments = listOf(TrafficRecords.segment(cell, listOf(200, 200, 200, 200))),
            requests = listOf(
                TrafficRecords.request(cell.plusMillis(1), requestId = "req-good-1", path = "/api/doc"),
                TrafficRecords.request(cell.plusMillis(2), requestId = "req-query", path = "/search?q=MARKER-query"),
                TrafficRecords.request(cell.plusMillis(3), requestId = "req-address", path = "/ip/203.0.113.9"),
                TrafficRecords.request(cell.plusMillis(4), requestId = "req-good-2", path = "/docs/v1.2/time/12:30"),
            ),
        )
        val entry = accepted(fields(body))
        assertEquals(listOf("req-good-1", "req-good-2"), entry.requests.map { it.requestId })
        assertEquals(2L, entry.requestsRejected)
        assertEquals(4L, entry.segments.single().totals.count, "줄을 버리며 묶음을 건드렸다")
        assertFalse(entry.toString().contains("MARKER"))
        assertFalse(entry.toString().contains("203.0.113.9"))
    }

    @Test
    fun `경로 판정 표 — 날것 ? · %3F · 주소 꼴(IPv4 · 완전한 IPv6 · 겹콜론 꼴 · 대괄호 · 인코딩된 것)만 그 줄을 버리고 나머지는 받는다`() {
        val kept = listOf(
            "/api/doc", "/docs/v1.2/guide", "/v1.2.3", "/release/1.2.3", "/time/12:30", "/time/12:30:00", "/a:b:c", "/hex/dead:beef:cafe",
            "/wiki/C++::std", "/search;q=x", "/wiki/[draft]",
        )
        val rejected = listOf(
            "/search?q=x",                                  // 날것 ?
            "/search%3Fq=x", "/search%3fq=x",               // 인코딩된 ? — 대소문자 모두
            "/peer/10.0.0.1/status", "/release/1.2.3.4",    // IPv4 꼴 — 판 번호도(받아들인 한계)
            "/ip/2001:db8:0:0:0:0:2:1",                     // 묶음 여덟
            "/ip/fe80::1", "/ip/::1", "/ns/a::b", "/ip/::ffff:192.0.2.1",
            "/peer/[2001:db8::1]:8080",                     // 대괄호
            "/ip/fe80%3a%3a1", "/ip/FE80%3A%3A1",           // 인코딩된 콜론 — 대소문자 모두
            "/peer/%5b2001:db8::1%5d", "/peer/%5B2001:db8::1%5D", // 인코딩된 대괄호
            "/peer/[1:2]", "/peer/%5B1:2%5D", "/peer/%5b1%3a2%5d",       // 대괄호 안에 콜론 — 다른 주소 꼴이 없어도
        )
        val failures = mutableListOf<String>()
        for (path in kept + rejected) {
            val entry = accepted(fields(TrafficRecords.body(
                segments = listOf(TrafficRecords.segment(cell, listOf(200, 200))),
                requests = listOf(TrafficRecords.request(cell, requestId = "req-other", path = "/api/other"), TrafficRecords.request(cell, requestId = "req-judged", path = path)),
            )))
            val expectRejected = path in rejected
            val stored = entry.requests.map { it.requestId }
            val ok = if (expectRejected) stored == listOf("req-other") && entry.requestsRejected == 1L else stored == listOf("req-other", "req-judged") && entry.requestsRejected == 0L
            if (!ok) failures += "$path → ${if (expectRejected) "버려야 한다" else "받아야 한다"}(남은 줄 $stored · 버림 ${entry.requestsRejected})"
            if (entry.segments.single().totals.count != 2L) failures += "$path → 같은 항목의 묶음이 달라졌다"
        }
        assertEquals(emptyList<String>(), failures)
    }

    @Test
    fun `한 칸의 수는 10억까지 받고 10억 1 이면 꼴 틀림으로 항목을 버린다`() {
        fun segmentOf(count: Long) = """{"bucket_start_ms": ${cell.toEpochMilli()}, "service": "backend", "caller": "front", "class": "request", "count": $count,
            "status": {"2xx": $count, "3xx": 0, "4xx": 0, "5xx": 0, "other": 0}, "buckets": [$count${", 0".repeat(19)}], "max_ms": 3}"""
        assertEquals(1_000_000_000L, accepted(fields(TrafficRecords.body(segments = listOf(segmentOf(1_000_000_000))))).segments.single().totals.count)
        assertDropped(TrafficDropReason.INVALID_BODY, fields(TrafficRecords.body(segments = listOf(segmentOf(1_000_000_001)))), "count · 분포 칸이 10억 1")
        // 분포의 한 칸만 넘는 꼴(합은 맞지 않아도 먼저 상한으로 걸린다)
        val overBucket = segmentOf(1_000_000_000).replace("\"buckets\": [1000000000, 0,", "\"buckets\": [1000000001, 0,")
        assertDropped(TrafficDropReason.INVALID_BODY, fields(TrafficRecords.body(segments = listOf(overBucket))), "분포의 한 칸이 10억 1")
    }

    @Test
    fun `다른 종류는 버리지 않고 OtherKind`() {
        assertEquals(TrafficEntryParse.OtherKind("resource"), parse(mapOf("kind" to "resource", "v" to "1")))
    }

    @Test
    fun `at 이 아무리 앞서거나 오래돼도 해석은 항목을 버리지 않는다`() {
        for (at in listOf(receivedAt.plus(Duration.ofHours(1)), receivedAt.minus(Duration.ofDays(30)))) {
            assertEquals(at, accepted(fields(at = at)).readAt, "at=$at")
        }
    }

    @Test
    fun `칸 · 요청별 줄이 앞서거나 오래돼도 해석은 버리지 않는다 — 저장이 순번 다음에 칸마다 거른다`() {
        val ahead = receivedAt.plus(Duration.ofMinutes(10))
        val old = receivedAt.minus(Duration.ofDays(10))
        val body = TrafficRecords.body(
            segments = listOf(TrafficRecords.segment(TrafficRecords.cellStart(ahead), listOf(200)), TrafficRecords.segment(TrafficRecords.cellStart(old), listOf(200))),
            requests = listOf(TrafficRecords.request(ahead), TrafficRecords.request(old)),
        )
        val entry = accepted(fields(body))
        assertEquals(2, entry.segments.size)
        assertEquals(2, entry.requests.size)
    }
}
