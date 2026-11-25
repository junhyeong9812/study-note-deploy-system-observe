package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/**
 * 명세 §10.2 스트림 항목(`kind`·`v`·`server`·`at`·`body`)과 §11.2 깨진 항목 · 시계 앞섬, §4 다른 종류.
 * 해석은 순수 함수 — 수신 시각을 인자로 준다.
 */
class MetricEntryParserTest {
    private val receivedAt = Instant.parse("2026-11-24T03:00:00Z")
    private val measuredAt = Instant.parse("2026-11-24T02:59:59.250Z")
    private val id = "1763953199250-3"

    private val validBody = """
        {
          "interval_ms": 30012,
          "host": { "cpu_pct": 3.4, "load1": 0.11, "mem_total": 16000000000, "mem_used": 2100000000,
                    "disk_total": 500235124736, "disk_used": 80570650624,
                    "net_rx_bps": 1200, "net_tx_bps": 3400, "io_read_bps": 0, "io_write_bps": 52000 },
          "containers": [ { "name": "front-app-green", "id": "0123456789ab", "project": "study-note-deploy-system-front",
                            "cpu_pct": 0.7, "mem_used": 119000000, "restarts": 0, "state": "running" } ],
          "gpus": [ { "index": 0, "util_pct": 0, "mem_used": 9437184, "mem_total": 8589934592, "temp_c": 28 } ],
          "missing": ["gpus"]
        }
    """.trimIndent()

    private fun record(
        kind: String? = "resource", version: String? = "1", server: String? = "ci-agent-158",
        at: String? = measuredAt.toEpochMilli().toString(), body: String? = validBody, recordId: String = id,
    ) = MetricStreamRecord(recordId, buildMap {
        kind?.let { put("kind", it) }
        version?.let { put("v", it) }
        server?.let { put("server", it) }
        at?.let { put("at", it) }
        body?.let { put("body", it) }
    })

    private fun parse(record: MetricStreamRecord, at: Instant = receivedAt) = MetricEntryParser.parse(record, at)
    private fun dropped(record: MetricStreamRecord) = (parse(record) as? MetricEntryParse.Dropped)?.reason
    private fun accepted(record: MetricStreamRecord): ResourceSample =
        (parse(record) as? MetricEntryParse.Accepted)?.sample ?: fail("받아들여야 한다: ${parse(record)}")

    /** 본문의 host 안 한 칸만 바꾼 본문 */
    private fun withHost(field: String, value: String) = validBody.replace(Regex("\"$field\": [^,}]+"), "\"$field\": $value")

    @Test
    fun `맞는 항목은 칸 전부가 그대로 해석되고 측정 시각과 수신 시각이 둘 다 남는다`() {
        val sample = accepted(record())
        assertEquals(ResourceSample(
            id = StreamId(1_763_953_199_250, 3),
            server = "ci-agent-158",
            measuredAt = measuredAt,
            receivedAt = receivedAt,
            intervalMs = 30_012,
            host = HostValues(3.4, 0.11, 16_000_000_000, 2_100_000_000, 500_235_124_736, 80_570_650_624, 1200.0, 3400.0, 0.0, 52000.0),
            containers = listOf(ContainerValues("front-app-green", "0123456789ab", "study-note-deploy-system-front", 0.7, 119_000_000, 0, "running")),
            gpus = listOf(GpuValues(0, 0.0, 9_437_184, 8_589_934_592, 28.0)),
            missing = listOf("gpus"),
        ), sample)
    }

    @Test
    fun `차이를 낼 수 없어 null 로 온 값은 null 로 받는다`() {
        val body = """{"interval_ms": null, "host": {"cpu_pct": null, "load1": 0.5, "mem_total": 100, "mem_used": 50, "disk_total": 10, "disk_used": 1,
            "net_rx_bps": null, "net_tx_bps": null, "io_read_bps": null, "io_write_bps": null},
            "containers": [{"name": "observe-app-blue", "id": "abcdefabcdef", "project": "study-note-deploy-system-observe", "cpu_pct": null, "mem_used": 10, "restarts": 2, "state": "running"}],
            "gpus": [], "missing": []}"""
        val sample = accepted(record(body = body))
        assertNull(sample.intervalMs)
        assertEquals(HostValues(null, 0.5, 100, 50, 10, 1, null, null, null, null), sample.host)
        assertNull(sample.containers.single().cpuPct)
        assertEquals(2L, sample.containers.single().restarts)
    }

    @Test
    fun `읽지 못한 갈래가 비어 와도 나머지를 받는다 — host 가 없으면 null, 목록이 없으면 빈 목록`() {
        val sample = accepted(record(body = """{"interval_ms": 30000, "missing": ["host", "containers"]}"""))
        assertNull(sample.host)
        assertEquals(emptyList<ContainerValues>(), sample.containers)
        assertEquals(emptyList<GpuValues>(), sample.gpus)
        assertEquals(listOf("host", "containers"), sample.missing)
        assertEquals(emptyList<String>(), accepted(record(body = """{"host": null}""")).missing)
    }

    @Test
    fun `compose 프로젝트가 없는 컨테이너(설정으로 더한 것)는 project 가 빈 글자로 온다`() {
        val body = """{"containers": [{"name": "claude-bridge-relay", "id": "0123456789abcdef", "project": "", "cpu_pct": 1.5, "mem_used": 3, "restarts": 0, "state": "running"}]}"""
        assertEquals("", accepted(record(body = body)).containers.single().project)
    }

    @Test
    fun `본문의 모르는 칸은 무시한다 — 뒤에 칸이 늘어도 받는다`() {
        val body = validBody.replaceFirst("{", """{"future_field": {"x": [1, 2]}, """)
        assertEquals(accepted(record()).host, accepted(record(body = body)).host)
    }

    // ── 다른 종류(§4) ─────────────────────────────────────

    @Test
    fun `kind 가 resource 가 아니면 버린 것이 아니라 다른 종류이고 나머지 칸은 보지 않는다`() {
        assertEquals(MetricEntryParse.OtherKind("trace"), parse(record(kind = "trace")))
        assertEquals(MetricEntryParse.OtherKind("trace"), parse(record(kind = "trace", version = "9", server = null, at = "x", body = "{")))
        assertEquals(MetricEntryParse.OtherKind("Resource"), parse(record(kind = "Resource")), "글자는 정확히 같아야 한다")
    }

    // ── 깨진 항목 ─────────────────────────────────────────

    @Test
    fun `칸이 빠지면 missing_field`() {
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(kind = null)))
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(version = null)))
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(server = null)))
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(at = null)))
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(body = null)))
        assertEquals(MetricDropReason.MISSING_FIELD, dropped(record(server = "")), "빈 글자도 빠진 것이다")
    }

    @Test
    fun `스트림 ID 꼴이 아니면 invalid_id, 칸이 하나도 없으면(상한에 밀려남) trimmed`() {
        assertEquals(MetricDropReason.INVALID_ID, dropped(record(recordId = "abc")))
        assertEquals(MetricDropReason.TRIMMED, dropped(MetricStreamRecord(id, emptyMap())))
    }

    @Test
    fun `v 가 모르는 값이면 unsupported_version`() {
        for (version in listOf("2", "v1", "1.0", "01")) assertEquals(MetricDropReason.UNSUPPORTED_VERSION, dropped(record(version = version)), version)
    }

    @Test
    fun `서버 이름 꼴이 아니면 invalid_server`() {
        for (server in listOf("CI-AGENT", "has space", "../etc", "-lead", "a".repeat(64), "ci/agent", "가나")) {
            assertEquals(MetricDropReason.INVALID_SERVER, dropped(record(server = server)), server)
        }
        for (server in listOf("ci-agent-158", "ci-master", "a", "a".repeat(63), "host.local", "ci_agent_9")) {
            assertEquals(server, accepted(record(server = server)).server, server)
        }
    }

    @Test
    fun `at 이 epoch 밀리초가 아니면 invalid_at`() {
        for (at in listOf("abc", "-5", "1.5", "0", "2026-11-24T03:00:00Z", "99999999999999999999")) {
            assertEquals(MetricDropReason.INVALID_AT, dropped(record(at = at)), at)
        }
    }

    @Test
    fun `측정 시각이 수신 시각보다 2분 넘게 앞서면 clock_ahead 이고 정확히 2분은 받는다`() {
        val twoMinutesAhead = receivedAt.plus(Duration.ofMinutes(2))
        assertEquals(twoMinutesAhead, accepted(record(at = twoMinutesAhead.toEpochMilli().toString())).measuredAt)
        assertEquals(MetricDropReason.CLOCK_AHEAD, dropped(record(at = twoMinutesAhead.plusMillis(1).toEpochMilli().toString())))
        // 늦게 온 것(뒤처진 시계·밀린 항목)은 받는다
        val dayLate = receivedAt.minus(Duration.ofDays(1))
        assertEquals(dayLate, accepted(record(at = dayLate.toEpochMilli().toString())).measuredAt)
    }

    @Test
    fun `본문은 UTF-8 로 64KB 까지 받고 넘으면 body_too_large`() {
        val atLimit = validBody.dropLast(1) + " ".repeat(MetricEntryParser.MAX_BODY_BYTES - validBody.toByteArray().size) + "}"
        assertEquals(MetricEntryParser.MAX_BODY_BYTES, atLimit.toByteArray().size)
        accepted(record(body = atLimit))
        assertEquals(MetricDropReason.BODY_TOO_LARGE, dropped(record(body = atLimit.dropLast(1) + " }")))
        // 글자 수가 아니라 바이트 수 — 한글 한 글자는 3바이트
        val korean = validBody.replaceFirst("{", """{"note": "${"가".repeat(22_000)}", """)
        assertTrue(korean.length < MetricEntryParser.MAX_BODY_BYTES && korean.toByteArray().size > MetricEntryParser.MAX_BODY_BYTES)
        assertEquals(MetricDropReason.BODY_TOO_LARGE, dropped(record(body = korean)))
    }

    @Test
    fun `본문이 JSON 이 아니면 body_not_json — NaN 같은 JSON 밖의 낱말 포함`() {
        for (body in listOf("{", "not json", """{"host": {"cpu_pct": NaN}}""", """{"host": {"cpu_pct": Infinity}}""", """{} {}""")) {
            assertEquals(MetricDropReason.BODY_NOT_JSON, dropped(record(body = body)), body)
        }
    }

    @Test
    fun `본문이 객체가 아니거나 갈래의 꼴이 다르면 invalid_body`() {
        val bodies = listOf(
            "[]", "\"x\"", "null", "3",
            """{"host": [1]}""", """{"host": 3}""",
            """{"containers": {}}""", """{"containers": [3]}""", """{"gpus": "0"}""",
            """{"missing": "gpus"}""", """{"missing": [1]}""",
            """{"containers": [{"id": "0123456789ab", "project": "p", "state": "running"}]}""",                          // 이름 없음
            """{"containers": [{"name": "has space", "id": "0123456789ab", "project": "p", "state": "running"}]}""",
            """{"containers": [{"name": "/front-app", "id": "0123456789ab", "project": "p", "state": "running"}]}""",
            """{"containers": [{"name": "front-app", "id": "xyz", "project": "p", "state": "running"}]}""",           // ID 꼴
            """{"containers": [{"name": "front-app", "id": "0123456789ab", "project": "p", "state": "run\u0000ning"}]}""",
            """{"gpus": [{"index": -1}]}""", """{"gpus": [{"index": "0"}]}""", """{"gpus": [{"util_pct": 3}]}""",
        )
        for (body in bodies) assertEquals(MetricDropReason.INVALID_BODY, dropped(record(body = body)), body)
    }

    @Test
    fun `값이 음수·범위 밖·숫자가 아니면 invalid_value`() {
        val bad = listOf(
            withHost("cpu_pct", "-0.1"), withHost("cpu_pct", "100.1"), withHost("cpu_pct", "\"NaN\""), withHost("cpu_pct", "\"3\""),
            withHost("cpu_pct", "true"), withHost("mem_used", "-1"), withHost("mem_total", "1.5"), withHost("load1", "-0.01"),
            withHost("net_rx_bps", "-1"), withHost("disk_used", "\"80\""),
            validBody.replace("\"interval_ms\": 30012", "\"interval_ms\": -1"),
            validBody.replace("\"cpu_pct\": 0.7", "\"cpu_pct\": -1"),
            validBody.replace("\"restarts\": 0", "\"restarts\": -1"),
            validBody.replace("\"mem_used\": 119000000", "\"mem_used\": -5"),
            validBody.replace("\"util_pct\": 0", "\"util_pct\": 100.5"),
            validBody.replace("\"temp_c\": 28", "\"temp_c\": 151"),
            validBody.replace("\"mem_total\": 8589934592", "\"mem_total\": -8"),
        )
        for (body in bad) {
            assertNotEquals(validBody, body, "바꾸려던 칸이 본문에 없다")
            assertEquals(MetricDropReason.INVALID_VALUE, dropped(record(body = body)), body)
        }
    }

    @Test
    fun `경계 안의 값은 받는다 — 서버 CPU 0·100, 컨테이너 CPU 는 코어 하나가 100 이라 100 을 넘어도 된다`() {
        assertEquals(100.0, accepted(record(body = withHost("cpu_pct", "100"))).host!!.cpuPct)
        assertEquals(0.0, accepted(record(body = withHost("cpu_pct", "0"))).host!!.cpuPct)
        assertEquals(250.0, accepted(record(body = validBody.replace("\"cpu_pct\": 0.7", "\"cpu_pct\": 250"))).containers.single().cpuPct)
        assertEquals(100.0, accepted(record(body = validBody.replace("\"util_pct\": 0", "\"util_pct\": 100"))).gpus.single().utilPct)
    }
}

/** 명세 §11.5 — 조회의 컨테이너 이름은 자리 이름(`-blue`·`-green`)을 뗀 이름 */
class ContainerNameTest {
    @Test
    fun `끝의 -blue · -green 을 뗀다`() {
        assertEquals("front-app", ContainerName.seriesName("front-app-green"))
        assertEquals("front-app", ContainerName.seriesName("front-app-blue"))
        assertEquals("observe-app", ContainerName.seriesName("observe-app-blue"))
        assertEquals("front-app-blue", ContainerName.seriesName("front-app-blue-green"), "하나만 뗀다")
    }

    @Test
    fun `그 꼴로 끝나지 않거나 떼면 빈 이름이 되는 이름은 그대로`() {
        for (name in listOf("green-app", "blue", "green", "-green", "app-greenish", "app-Green", "app_green", "appgreen", "claude-bridge-relay", "llm-ollama", "observe-postgres")) {
            assertEquals(name, ContainerName.seriesName(name), name)
        }
    }
}
