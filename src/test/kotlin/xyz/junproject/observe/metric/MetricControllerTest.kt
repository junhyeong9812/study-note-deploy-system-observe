package xyz.junproject.observe.metric

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.dao.QueryTimeoutException
import org.springframework.http.ResponseEntity
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.log.StreamId
import java.time.Duration
import java.time.Instant

/**
 * 명세 §10.3 · §11.5 · §11.6 — 조회 경로 셋: 전부 `X-Observe-Secret`(입력 검사보다 먼저), 입력이 틀리면 422, 응답의 칸 이름.
 * 저장소는 Fake — 경계에서 저장소로 넘어가는 조건과 응답의 꼴만 본다(실제 저장소를 거친 것은 엔진 테스트).
 */
class MetricControllerTest {
    private val now = Instant.parse("2026-11-24T03:00:00Z")

    private class FakeMetricReader : MetricReader {
        val seriesQueries = mutableListOf<SeriesQuery>()
        val latestAsked = mutableListOf<Instant>()
        val containersAsked = mutableListOf<Triple<String, Instant, Instant>>()
        var points: (SeriesQuery) -> List<SeriesPoint> = { emptyList() }
        var latestSamples: List<ResourceSample> = emptyList()
        var seen: List<SeenContainer> = emptyList()
        var failure: RuntimeException? = null
        val touched get() = seriesQueries.size + latestAsked.size + containersAsked.size

        override fun series(query: SeriesQuery): List<SeriesPoint> { failure?.let { throw it }; seriesQueries += query; return points(query) }
        override fun rollups(server: String, scope: MetricScope, name: String?, from: Instant, to: Instant): List<MetricRollup> = emptyList()
        override fun latest(receivedSince: Instant): List<ResourceSample> { failure?.let { throw it }; latestAsked += receivedSince; return latestSamples }
        override fun containers(server: String, from: Instant, to: Instant): List<SeenContainer> {
            failure?.let { throw it }; containersAsked += Triple(server, from, to); return seen
        }
    }

    private val reader = FakeMetricReader()
    private val controller = MetricController(reader, "the-secret", "")

    private fun request(secret: String? = "the-secret") = MockHttpServletRequest("GET", "/metrics").apply { if (secret != null) addHeader("X-Observe-Secret", secret) }

    private fun series(
        server: String? = "ci-agent-158", scope: String? = "host", name: String? = null, from: String? = null, to: String? = null,
        secret: String? = "the-secret", target: MetricController = controller,
    ) = target.series(request(secret), server, scope, name, from, to, now)

    @Suppress("UNCHECKED_CAST")
    private fun data(response: ResponseEntity<out Any>): Map<String, Any?> {
        assertEquals(200, response.statusCode.value(), "${response.body}")
        val body = response.body as Map<String, Any?>
        assertEquals(true, body["success"])
        return body["data"] as Map<String, Any?>
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertInvalid(response: ResponseEntity<out Any>, label: String) {
        assertEquals(422, response.statusCode.value(), "$label: ${response.body}")
        val error = (response.body as Map<String, Any?>)["error"] as Map<String, Any?>
        assertEquals("invalid_request", error["code"], label)
        assertTrue((error["detail"] as? String)?.isNotBlank() == true, "$label: detail 이 없다")
    }

    private val unauthorized = mapOf("success" to false, "error" to mapOf("code" to "unauthorized"))

    // ── 인증 ──────────────────────────────────────────────

    @Test
    fun `세 경로 모두 비밀값이 없거나 틀리면 401 이고 입력이 틀려도 422 보다 먼저다`() {
        for (secret in listOf(null, "wrong", "")) {
            val responses = listOf(
                controller.latest(request(secret), now),
                series(secret = secret),
                series(secret = secret, server = null, scope = "bogus", from = "어제"),              // 입력도 틀렸다
                controller.containers(request(secret), "ci-agent-158", null, now),
                controller.containers(request(secret), null, "어제", now),
            )
            for (response in responses) {
                assertEquals(401, response.statusCode.value(), "secret=$secret ${response.body}")
                assertEquals(unauthorized, response.body)
            }
        }
        assertEquals(0, reader.touched, "인증 전에 저장소를 불렀다")
    }

    @Test
    fun `설정된 비밀값이 비어 있으면 빈 헤더로도 열리지 않는다`() {
        val unconfigured = MetricController(reader, "", "")
        assertEquals(401, unconfigured.latest(request(""), now).statusCode.value())
        assertEquals(401, series(secret = "", target = unconfigured).statusCode.value())
        assertEquals(401, unconfigured.containers(request(null), "ci-agent-158", null, now).statusCode.value())
    }

    // ── GET /metrics/series ─────────────────────────────

    @Test
    fun `기간을 주지 않으면 최근 1시간을 30초 간격 원본으로 묻고 응답에 간격과 읽은 곳을 적는다`() {
        val body = data(series())
        assertEquals(SeriesQuery("ci-agent-158", MetricScope.HOST, null, now.minus(Duration.ofHours(1)), now, Duration.ofSeconds(30),
            now.minus(Duration.ofDays(7))), reader.seriesQueries.single())
        assertEquals("ci-agent-158", body["server"])
        assertEquals("host", body["scope"])
        assertTrue(body.containsKey("name") && body["name"] == null, "host 의 name 은 null: $body")
        assertEquals(now.minus(Duration.ofHours(1)).toString(), body["from"])
        assertEquals(now.toString(), body["to"])
        assertEquals(30_000L, (body["step_ms"] as Number).toLong())
        assertEquals("raw", body["source"])
        assertEquals(emptyList<Any>(), body["points"], "값이 없으면 점도 없다")
    }

    @Test
    fun `from 만 있으면 지금까지, to 만 있으면 그 앞 1시간`() {
        series(from = "2026-11-24T01:00:00Z")
        assertEquals(Instant.parse("2026-11-24T01:00:00Z") to now, reader.seriesQueries.last().let { it.from to it.to })
        series(to = "2026-11-23T10:00:00Z")
        assertEquals(Instant.parse("2026-11-23T09:00:00Z") to Instant.parse("2026-11-23T10:00:00Z"), reader.seriesQueries.last().let { it.from to it.to })
    }

    @Test
    fun `점은 그 갈래의 값마다 평균과 _max 를 내고 표본이 없는 값은 null`() {
        val at = now.minus(Duration.ofMinutes(10))
        reader.points = { listOf(SeriesPoint(at, mapOf("cpu_pct" to MetricAggregate(30.0, 60.0, 3), "mem_used" to MetricAggregate(2e9, 2.5e9, 3)))) }
        @Suppress("UNCHECKED_CAST")
        val point = (data(series())["points"] as List<Map<String, Any?>>).single()
        val expectedKeys = setOf("at") + MetricScope.HOST.values.flatMap { listOf(it, "${it}_max") }
        assertEquals(expectedKeys, point.keys)
        assertEquals(at.toString(), point["at"])
        assertEquals(30.0, (point["cpu_pct"] as Number).toDouble())
        assertEquals(60.0, (point["cpu_pct_max"] as Number).toDouble())
        assertEquals(2e9, (point["mem_used"] as Number).toDouble())
        assertEquals(2.5e9, (point["mem_used_max"] as Number).toDouble())
        assertNull(point["load1"])
        assertNull(point["load1_max"])
    }

    @Test
    fun `컨테이너와 GPU 의 점 칸은 그 갈래의 값 이름이다`() {
        reader.points = { listOf(SeriesPoint(now.minusSeconds(60), mapOf("cpu_pct" to MetricAggregate(1.0, 1.0, 1)))) }
        @Suppress("UNCHECKED_CAST")
        val containerPoint = (data(series(scope = "container", name = "front-app"))["points"] as List<Map<String, Any?>>).single()
        assertEquals(setOf("at", "cpu_pct", "cpu_pct_max", "mem_used", "mem_used_max"), containerPoint.keys)
        assertEquals(SeriesQuery("ci-agent-158", MetricScope.CONTAINER, "front-app", now.minus(Duration.ofHours(1)), now, Duration.ofSeconds(30),
            now.minus(Duration.ofDays(7))), reader.seriesQueries.last())

        @Suppress("UNCHECKED_CAST")
        val gpuPoint = (data(series(scope = "gpu", name = "0"))["points"] as List<Map<String, Any?>>).single()
        assertEquals(setOf("at") + MetricScope.GPU.values.flatMap { listOf(it, "${it}_max") }, gpuPoint.keys)
        assertEquals("0", reader.seriesQueries.last().name)
    }

    @Test
    fun `오래된 기간은 rollup, 경계를 걸치면 mixed 이고 간격이 응답에 나온다`() {
        val old = data(series(from = "2026-11-04T03:00:00Z", to = "2026-11-14T03:00:00Z"))   // 10일 ÷ 500 = 1,728초 → 1,740초
        assertEquals("rollup", old["source"])
        assertEquals(1_740_000L, (old["step_ms"] as Number).toLong())
        assertEquals(Duration.ofSeconds(1_740), reader.seriesQueries.last().step)

        val crossing = data(series(from = "2026-11-16T03:00:00Z", to = "2026-11-18T03:00:00Z"))
        assertEquals("mixed", crossing["source"])
        assertEquals(360_000L, (crossing["step_ms"] as Number).toLong())
    }

    @Test
    fun `정확히 31일은 받고 넘으면 422`() {
        assertEquals(200, series(from = now.minus(Duration.ofDays(31)).toString(), to = now.toString()).statusCode.value())
        assertInvalid(series(from = now.minus(Duration.ofDays(31)).minusMillis(1).toString(), to = now.toString()), "31일 + 1ms")
    }

    @Test
    fun `series 의 입력이 틀리면 422 이고 저장소에 닿지 않는다`() {
        val cases = listOf(
            "server 없음" to series(server = null),
            "server 빈 글자" to series(server = ""),
            "server 꼴" to series(server = "CI AGENT"),
            "scope 없음" to series(scope = null),
            "scope 모름" to series(scope = "disk"),
            "scope 대문자" to series(scope = "HOST"),
            "host 에 name" to series(scope = "host", name = "x"),
            "container 에 name 없음" to series(scope = "container", name = null),
            "container name 꼴" to series(scope = "container", name = "front app"),
            "container name 128자 초과" to series(scope = "container", name = "a".repeat(129)),
            "gpu 에 name 없음" to series(scope = "gpu", name = null),
            "gpu name 이 숫자가 아님" to series(scope = "gpu", name = "first"),
            "gpu name 네 자리" to series(scope = "gpu", name = "1000"),
            "from 꼴" to series(from = "어제"),
            "to 꼴" to series(to = "1763953199250"),
            "거꾸로" to series(from = "2026-11-24T02:00:00Z", to = "2026-11-24T01:00:00Z"),
        )
        for ((label, response) in cases) assertInvalid(response, label)
        assertTrue(reader.seriesQueries.isEmpty(), "422 인데 저장소를 불렀다")
    }

    @Test
    fun `조회 중 DB 오류는 503 query_failed, 시간 초과는 504 query_timeout 이고 예외 문구를 내지 않는다`() {
        reader.failure = DataAccessResourceFailureException("Connection to postgres:5432 refused — 내부 문구")
        for (response in listOf(series(), controller.latest(request(), now), controller.containers(request(), "ci-agent-158", null, now))) {
            assertEquals(503, response.statusCode.value())
            assertEquals(mapOf("success" to false, "error" to mapOf("code" to "query_failed")), response.body)
        }
        reader.failure = QueryTimeoutException("canceling statement due to statement timeout — 내부 문구")
        assertEquals(504, series().statusCode.value())
        assertEquals(mapOf("success" to false, "error" to mapOf("code" to "query_timeout")), series().body)
    }

    // ── GET /metrics/latest ─────────────────────────────

    private fun sample(server: String, receivedAgo: Duration, measuredBeforeReceipt: Duration = Duration.ofMillis(800)) = ResourceSample(
        id = StreamId(now.minus(receivedAgo).toEpochMilli(), 0),
        server = server,
        measuredAt = now.minus(receivedAgo).minus(measuredBeforeReceipt),
        receivedAt = now.minus(receivedAgo),
        intervalMs = 30_000,
        host = HostValues(cpuPct = 3.4, load1 = 0.11, memTotal = 16_000_000_000, memUsed = 2_100_000_000, diskTotal = 500, diskUsed = 80,
            netRxBps = 1200.0, netTxBps = 3400.0, ioReadBps = 0.0, ioWriteBps = null),
        containers = listOf(ContainerValues("front-app-green", "0123456789ab", "study-note-deploy-system-front", 0.7, 119_000_000, 2, "running")),
        gpus = listOf(GpuValues(0, 5.0, 9_437_184, 8_589_934_592, 28.0)),
        missing = listOf("gpus"),
    )

    @Suppress("UNCHECKED_CAST")
    private fun servers(response: ResponseEntity<out Any>) = data(response)["servers"] as List<Map<String, Any?>>

    @Test
    fun `latest 는 최근 24시간에 받은 서버를 묻고 칸을 명세 이름 그대로 내며 firing 은 빈 배열이다`() {
        reader.latestSamples = listOf(sample("ci-agent-158", Duration.ofSeconds(20)))
        val body = data(controller.latest(request(), now))
        assertEquals(listOf(now.minus(Duration.ofHours(24))), reader.latestAsked)
        assertEquals(emptyList<Any>(), body["firing"])
        @Suppress("UNCHECKED_CAST")
        val server = (body["servers"] as List<Map<String, Any?>>).single()
        assertEquals(setOf("server", "at", "received_at", "age_ms", "stale", "host", "containers", "gpus", "missing"), server.keys)
        assertEquals("ci-agent-158", server["server"])
        assertEquals(now.minusSeconds(20).minusMillis(800).toString(), server["at"], "at 은 측정 시각")
        assertEquals(now.minusSeconds(20).toString(), server["received_at"])
        assertEquals(20_000L, (server["age_ms"] as Number).toLong(), "age_ms 는 수신 시각부터")
        assertEquals(false, server["stale"])
        @Suppress("UNCHECKED_CAST")
        val host = server["host"] as Map<String, Any?>
        assertEquals(MetricScope.HOST.values.toSet(), host.keys)
        assertEquals(3.4, (host["cpu_pct"] as Number).toDouble())
        assertEquals(16_000_000_000L, (host["mem_total"] as Number).toLong())
        assertNull(host["io_write_bps"])
        @Suppress("UNCHECKED_CAST")
        val container = (server["containers"] as List<Map<String, Any?>>).single()
        assertEquals(setOf("name", "series_name", "id", "project", "cpu_pct", "mem_used", "restarts", "state"), container.keys)
        assertEquals("front-app-green", container["name"])
        assertEquals("front-app", container["series_name"])
        assertEquals(2L, (container["restarts"] as Number).toLong())
        assertEquals("running", container["state"])
        @Suppress("UNCHECKED_CAST")
        val gpu = (server["gpus"] as List<Map<String, Any?>>).single()
        assertEquals(setOf("index", "util_pct", "mem_used", "mem_total", "temp_c"), gpu.keys)
        assertEquals(0L, (gpu["index"] as Number).toLong())
        assertEquals(listOf("gpus"), server["missing"])
    }

    @Test
    fun `수신이 3분 넘게 지나면 stale — 정확히 3분은 아니다`() {
        reader.latestSamples = listOf(sample("a-server", Duration.ofMinutes(3)), sample("b-server", Duration.ofMinutes(3).plusMillis(1)))
        val byName = servers(controller.latest(request(), now)).associateBy { it["server"] }
        assertEquals(false, byName["a-server"]!!["stale"])
        assertEquals(180_000L, (byName["a-server"]!!["age_ms"] as Number).toLong())
        assertEquals(true, byName["b-server"]!!["stale"])
    }

    @Test
    fun `기대 서버 중 받은 것이 없는 서버는 수신 없음으로 나오고 서버는 이름순이다`() {
        reader.latestSamples = listOf(sample("ci-agent-9", Duration.ofSeconds(5)), sample("ci-agent-158", Duration.ofSeconds(5)))
        val expecting = MetricController(reader, "the-secret", " ci-agent-164, ci-agent-158 ,,")
        val listed = servers(expecting.latest(request(), now))
        assertEquals(listOf("ci-agent-158", "ci-agent-164", "ci-agent-9"), listed.map { it["server"] })
        val silent = listed[1]
        assertEquals(mapOf("server" to "ci-agent-164", "at" to null, "received_at" to null, "age_ms" to null, "stale" to true,
            "host" to null, "containers" to emptyList<Any>(), "gpus" to emptyList<Any>(), "missing" to emptyList<Any>()), silent)
    }

    @Test
    fun `기대 서버 설정이 비어 있으면 받은 서버만 나온다`() {
        reader.latestSamples = emptyList()
        assertEquals(emptyList<Any>(), servers(controller.latest(request(), now)))
    }

    // ── GET /metrics/containers ─────────────────────────

    @Test
    fun `at 이 없으면 최근 24시간, 있으면 그 시각 앞뒤 5분을 묻는다`() {
        reader.seen = listOf(SeenContainer("front-app", now.minusSeconds(40)), SeenContainer("observe-app", now.minusSeconds(70)))
        val body = data(controller.containers(request(), "ci-agent-158", null, now))
        assertEquals(Triple("ci-agent-158", now.minus(Duration.ofHours(24)), now), reader.containersAsked.last())
        assertEquals("ci-agent-158", body["server"])
        assertEquals(now.minus(Duration.ofHours(24)).toString(), body["from"])
        assertEquals(now.toString(), body["to"])
        assertEquals(listOf(
            mapOf("name" to "front-app", "last_seen_at" to now.minusSeconds(40).toString()),
            mapOf("name" to "observe-app", "last_seen_at" to now.minusSeconds(70).toString()),
        ), body["containers"])

        val at = Instant.parse("2026-11-23T21:13:00Z")
        val around = data(controller.containers(request(), "ci-agent-158", at.toString(), now))
        assertEquals(Triple("ci-agent-158", at.minus(Duration.ofMinutes(5)), at.plus(Duration.ofMinutes(5))), reader.containersAsked.last())
        assertEquals(at.minus(Duration.ofMinutes(5)).toString(), around["from"])
        assertEquals(at.plus(Duration.ofMinutes(5)).toString(), around["to"])
    }

    @Test
    fun `containers 의 입력이 틀리면 422 이고 저장소에 닿지 않는다`() {
        assertInvalid(controller.containers(request(), null, null, now), "server 없음")
        assertInvalid(controller.containers(request(), "Bad Server", null, now), "server 꼴")
        assertInvalid(controller.containers(request(), "ci-agent-158", "어제", now), "at 꼴")
        assertTrue(reader.containersAsked.isEmpty())
    }
}
