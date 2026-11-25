package xyz.junproject.observe.engine

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.mock.web.MockHttpServletRequest
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.ContainerValues
import xyz.junproject.observe.metric.GpuValues
import xyz.junproject.observe.metric.HostValues
import xyz.junproject.observe.metric.MetricAggregate
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricDeletion
import xyz.junproject.observe.metric.MetricScope
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.ResourceSample
import xyz.junproject.observe.metric.SeriesQuery
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * 실제 PostgreSQL — 명세 §2.3 · §11.2 · §11.3 · §11.5 · §11.6, ⑤ 자동(observe):
 * 같은 항목 한 번만 · 측정/수신 시각 둘 다 · 5분 평균(평균·최댓값·표본 수·경계·null·늦은 도착·반복) ·
 * 원본 7일 삭제는 평균이 마지막 수신보다 뒤에 계산된 구간만 · 평균 30일 · 조회(점·간격·source·구멍·자리 합·가중 평균) ·
 * latest · containers. 시각은 모두 인자로 정한다(기다리지 않는다).
 *
 * 각 사례는 빈 전용 스키마(`metric_engine`)에서 돈다 — 구현의 표 이름을 몰라도 사례끼리 섞이지 않고 기존 표와도 섞이지 않는다.
 * 원본이 남아 있는가는 [MetricStore.containers](원본 항목에서 읽는다)로 본다 — 그래서 모든 묶음에 `probe-app` 컨테이너를 넣는다.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricStoreEngineTest {
    private val schema = "metric_engine"
    private val administration: HikariDataSource = EngineSupport.dataSource(size = 2)
    private val administrationJdbc = JdbcTemplate(administration)
    private val dataSource = HikariDataSource(HikariConfig().apply {
        jdbcUrl = EngineSupport.databaseUrl + "?currentSchema=${this@MetricStoreEngineTest.schema}"
        username = EngineSupport.databaseUser; password = EngineSupport.databasePassword; maximumPoolSize = 6
    })
    private val store = JdbcTemplate(dataSource).let { MetricStore(it, it, it) }
    private val now = Instant.parse("2026-11-24T03:00:00Z")          // 5분 경계
    private val sequence = AtomicInteger()

    @BeforeEach
    fun reset() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        administrationJdbc.execute("CREATE SCHEMA $schema")
        store.createSchema()
    }

    @AfterAll
    fun close() {
        administrationJdbc.execute("DROP SCHEMA IF EXISTS $schema CASCADE")
        dataSource.close()
        administration.close()
    }

    /** 원본이 남아 있는가를 보는 표식 — 값이 없어 평균에는 아무것도 보태지 않는다 */
    private val probe = ContainerValues("probe-app", "0000000000aa", "study-note-deploy-system-probe", null, null, null, "running")

    private fun sample(
        measuredAt: Instant, receivedAt: Instant = measuredAt.plusSeconds(1), server: String = "ci-agent-158",
        host: HostValues? = HostValues(cpuPct = 10.0), containers: List<ContainerValues> = emptyList(), gpus: List<GpuValues> = emptyList(),
        missing: List<String> = emptyList(),
    ) = ResourceSample(StreamId(receivedAt.toEpochMilli(), sequence.incrementAndGet()), server, measuredAt, receivedAt, 30_000, host,
        containers + probe, gpus, missing)

    private fun cpu(value: Double?) = HostValues(cpuPct = value, load1 = 0.5)

    /** 원본 항목이 그 측정 시각에 남아 있는가 */
    private fun rawAt(at: Instant, server: String = "ci-agent-158") = store.containers(server, at, at).isNotEmpty()

    private fun rollupsOf(from: Instant, to: Instant, scope: MetricScope = MetricScope.HOST, name: String? = null, server: String = "ci-agent-158") =
        store.rollups(server, scope, name, from, to)

    // ── 저장 ──────────────────────────────────────────────

    @Test
    fun `같은 항목(같은 스트림 ID)을 두 번 저장해도 한 번만 남고 처음 값 그대로다`() {
        val first = sample(now.minusSeconds(60), host = cpu(12.0))
        assertEquals(1, store.insertAll(listOf(first)))
        assertEquals(0, store.insertAll(listOf(first.copy(host = cpu(99.0)))), "다시 받은 같은 항목을 새로 셌다")
        val second = sample(now.minusSeconds(30), host = cpu(20.0))
        assertEquals(1, store.insertAll(listOf(first, second)), "이미 있는 항목이 섞인 묶음에서 새 것만 세어야 한다")
        store.rollUp(now)
        val bucket = rollupsOf(now.minus(Duration.ofMinutes(5)), now).single()
        assertEquals(MetricAggregate(16.0, 20.0, 2), bucket.values["cpu_pct"], "같은 항목이 두 번 들어가 표본이 늘었다")
    }

    @Test
    fun `측정 시각과 수신 시각을 둘 다 저장하고 묶음의 값이 그대로 돌아온다`() {
        val stored = sample(
            measuredAt = now.minusSeconds(95), receivedAt = now.minusSeconds(3),
            host = HostValues(3.4, 0.11, 16_000_000_000, 2_100_000_000, 500_235_124_736, 80_570_650_624, 1200.0, 3400.0, 0.0, null),
            containers = listOf(ContainerValues("front-app-green", "0123456789ab", "study-note-deploy-system-front", 0.7, 119_000_000, 2, "running")),
            gpus = listOf(GpuValues(0, 5.5, 9_437_184, 8_589_934_592, 28.0)),
            missing = listOf("containers_truncated"),
        )
        store.insertAll(listOf(stored))
        assertEquals(listOf(stored), store.latest(now.minus(Duration.ofHours(24))))
        assertEquals(now.minusSeconds(3), store.latestStoredAt(), "마지막 저장 시각은 수신 시각")
        assertEquals(mapOf("ci-agent-158" to now.minusSeconds(3)), store.lastReceivedByServer())
    }

    @Test
    fun `읽지 못한 갈래가 빈 묶음도 그대로 저장된다`() {
        val partial = sample(now.minusSeconds(10), host = null, missing = listOf("host", "gpus"))
        store.insertAll(listOf(partial))
        assertEquals(listOf(partial), store.latest(now.minus(Duration.ofHours(1))))
    }

    @Test
    fun `latest 는 서버마다 측정 시각이 가장 늦은 항목이고 수신이 기준보다 앞선 서버는 빠진다`() {
        val older = sample(now.minusSeconds(90), server = "ci-agent-158", host = cpu(1.0))
        val newer = sample(now.minusSeconds(30), receivedAt = now.minusSeconds(29), server = "ci-agent-158", host = cpu(2.0))
        val lateArrivalOfOld = sample(now.minusSeconds(200), receivedAt = now.minusSeconds(5), server = "ci-agent-158", host = cpu(3.0))
        val other = sample(now.minusSeconds(10), server = "ci-agent-164", host = cpu(4.0))
        val silent = sample(now.minus(Duration.ofHours(25)), server = "ci-agent-9")
        store.insertAll(listOf(older, newer, lateArrivalOfOld, other, silent))
        assertEquals(listOf(newer, other), store.latest(now.minus(Duration.ofHours(24))))
    }

    @Test
    fun `스키마를 두 번 만들어도 그대로이고 값이 남는다`() {
        store.insertAll(listOf(sample(now.minusSeconds(10))))
        store.createSchema()
        assertTrue(rawAt(now.minusSeconds(10)))
    }

    // ── 5분 평균 ─────────────────────────────────────────

    @Test
    fun `5분 평균은 값별 평균·최댓값·표본 수이고 null 은 빠지며 경계 시각의 표본은 뒤 구간에 든다`() {
        val bucket = Instant.parse("2026-11-24T02:00:00Z")
        store.insertAll(listOf(
            sample(bucket, host = cpu(10.0)),
            sample(bucket.plusSeconds(150), host = cpu(20.0)),
            sample(bucket.plusSeconds(180), host = cpu(null)),                         // 평균에도 표본 수에도 들지 않는다
            sample(bucket.plus(Duration.ofMinutes(5)).minusMillis(1), host = cpu(60.0)),
            sample(bucket.plus(Duration.ofMinutes(5)), host = cpu(100.0)),             // 경계 시각 = 다음 구간의 시작
        ))
        assertEquals(2, store.rollUp(now), "계산한 (서버, 구간) 수")
        val rollups = rollupsOf(bucket, bucket.plus(Duration.ofMinutes(10)))
        assertEquals(listOf(bucket, bucket.plus(Duration.ofMinutes(5))), rollups.map { it.bucketStart })
        assertEquals(MetricAggregate(30.0, 60.0, 3), rollups[0].values["cpu_pct"])
        assertEquals(MetricAggregate(0.5, 0.5, 4), rollups[0].values["load1"], "load1 은 null 이 아니어서 네 표본 모두 든다")
        assertEquals(MetricAggregate(100.0, 100.0, 1), rollups[1].values["cpu_pct"])
    }

    @Test
    fun `구간의 값이 모두 null 이면 그 값은 평균에 없다 — 0 으로 채우지 않는다`() {
        val bucket = Instant.parse("2026-11-24T02:10:00Z")
        store.insertAll(listOf(sample(bucket.plusSeconds(5), host = cpu(null)), sample(bucket.plusSeconds(35), host = cpu(null))))
        store.rollUp(now)
        val values = rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5))).single().values
        assertFalse("cpu_pct" in values, "$values")
        assertEquals(MetricAggregate(0.5, 0.5, 2), values["load1"])
    }

    @Test
    fun `끝나지 않은 구간은 아직 평균을 내지 않는다`() {
        val bucket = Instant.parse("2026-11-24T02:55:00Z")
        store.insertAll(listOf(sample(bucket.plusSeconds(10))))
        assertEquals(0, store.rollUp(now.minusSeconds(1)))
        assertEquals(emptyList<Any>(), rollupsOf(bucket, now))
        assertEquals(1, store.rollUp(now))
        assertEquals(1, rollupsOf(bucket, now).size)
    }

    @Test
    fun `컨테이너 평균은 자리 이름을 뗀 이름으로 묶고 같은 항목의 두 자리는 더한 것이 표본 하나다`() {
        val bucket = Instant.parse("2026-11-24T02:20:00Z")
        fun container(name: String, cpu: Double?, memory: Long?) = ContainerValues(name, "0123456789ab", "study-note-deploy-system-front", cpu, memory, 0, "running")
        store.insertAll(listOf(
            sample(bucket.plusSeconds(0), containers = listOf(container("front-app-blue", 10.0, 100), container("front-app-green", 5.0, 50))),
            sample(bucket.plusSeconds(30), containers = listOf(container("front-app-green", 20.0, 200), container("front-app-worker", 99.0, 999))),
            sample(bucket.plusSeconds(60), server = "ci-agent-164", containers = listOf(container("front-app-green", 70.0, 700))),
        ))
        store.rollUp(now)
        val frontApp = rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)), MetricScope.CONTAINER, "front-app").single().values
        assertEquals(MetricAggregate(17.5, 20.0, 2), frontApp["cpu_pct"])
        assertEquals(MetricAggregate(175.0, 200.0, 2), frontApp["mem_used"])
        assertEquals(MetricAggregate(99.0, 99.0, 1), rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)), MetricScope.CONTAINER, "front-app-worker").single().values["cpu_pct"])
        assertEquals(emptyList<Any>(), rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)), MetricScope.CONTAINER, "front-app-green"), "실제 이름으로는 묶지 않는다")
    }

    @Test
    fun `GPU 평균은 번호로 묶는다`() {
        val bucket = Instant.parse("2026-11-24T02:25:00Z")
        store.insertAll(listOf(
            sample(bucket.plusSeconds(1), gpus = listOf(GpuValues(0, 10.0, 100, 1000, 30.0), GpuValues(1, 90.0, 900, 1000, 70.0))),
            sample(bucket.plusSeconds(31), gpus = listOf(GpuValues(0, 30.0, 300, 1000, 40.0))),
        ))
        store.rollUp(now)
        val first = rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)), MetricScope.GPU, "0").single().values
        assertEquals(MetricAggregate(20.0, 30.0, 2), first["util_pct"])
        assertEquals(MetricAggregate(35.0, 40.0, 2), first["temp_c"])
        assertEquals(MetricAggregate(90.0, 90.0, 1), rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)), MetricScope.GPU, "1").single().values["util_pct"])
    }

    @Test
    fun `평균을 만든 뒤 그 구간에 원본이 늦게 들어오면 다음 바퀴에 다시 계산해 덮어쓰고 여러 번 돌려도 같다`() {
        val bucket = Instant.parse("2026-11-24T02:30:00Z")
        val firstRun = Instant.parse("2026-11-24T02:40:00Z")
        store.insertAll(listOf(sample(bucket.plusSeconds(10), receivedAt = bucket.plusSeconds(11), host = cpu(10.0))))
        assertEquals(1, store.rollUp(firstRun))
        assertEquals(MetricAggregate(10.0, 10.0, 1), rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5))).single().values["cpu_pct"])
        assertEquals(0, store.rollUp(firstRun.plusSeconds(30)), "바뀐 것이 없는 구간을 다시 계산했다")

        // 관측 서버가 밀린 항목을 늦게 받았다 — 측정은 그 구간, 수신은 평균 계산 뒤
        store.insertAll(listOf(sample(bucket.plusSeconds(200), receivedAt = firstRun.plusSeconds(60), host = cpu(40.0))))
        assertEquals(1, store.rollUp(firstRun.plusSeconds(90)))
        val recomputed = rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5)))
        assertEquals(1, recomputed.size, "덮어쓰지 않고 하나 더 만들었다")
        assertEquals(MetricAggregate(25.0, 40.0, 2), recomputed.single().values["cpu_pct"])

        repeat(3) { store.rollUp(firstRun.plusSeconds(120L + it)) }
        assertEquals(recomputed, rollupsOf(bucket, bucket.plus(Duration.ofMinutes(5))))
    }

    @Test
    fun `수신 시각과 계산 시각이 같은 구간은 다음 바퀴에 다시 계산한다 — 뒤가 아니면 끝난 것이 아니다`() {
        val bucket = Instant.parse("2026-11-24T02:35:00Z")
        val runAt = Instant.parse("2026-11-24T02:45:00Z")
        store.insertAll(listOf(sample(bucket.plusSeconds(1), receivedAt = runAt, host = cpu(10.0))))
        assertEquals(1, store.rollUp(runAt))
        assertEquals(1, store.rollUp(runAt.plusSeconds(1)), "계산 시각이 마지막 수신보다 뒤가 아닌데 끝난 것으로 봤다")
        assertEquals(0, store.rollUp(runAt.plusSeconds(2)))
    }

    // ── 보관 삭제 ────────────────────────────────────────

    @Test
    fun `원본은 7일 뒤 지우되 평균이 마지막 수신보다 뒤에 계산된 구간만 — 평균이 없거나 늦게 들어온 원본이 있으면 그 구간을 통째로 남긴다`() {
        val old = now.minus(Duration.ofDays(8))                                   // 5분 경계
        val rolledBucket = old
        val neverRolledBucket = old.plus(Duration.ofMinutes(5))
        val lateBucket = old.plus(Duration.ofMinutes(10))
        val firstRun = old.plus(Duration.ofHours(1))

        store.insertAll(listOf(
            sample(rolledBucket.plusSeconds(10), receivedAt = rolledBucket.plusSeconds(11)),
            sample(rolledBucket.plusSeconds(40), receivedAt = rolledBucket.plusSeconds(41)),
            sample(lateBucket.plusSeconds(10), receivedAt = lateBucket.plusSeconds(11)),
        ))
        store.rollUp(firstRun)
        store.insertAll(listOf(
            sample(neverRolledBucket.plusSeconds(10), receivedAt = firstRun.plusSeconds(5)),   // 평균이 아직 없는 구간
            sample(lateBucket.plusSeconds(70), receivedAt = firstRun.plusSeconds(5)),          // 평균보다 늦게 들어온 원본
        ))

        assertEquals(MetricDeletion(2, 0), store.deleteExpired(now))
        assertFalse(rawAt(rolledBucket.plusSeconds(10)))
        assertFalse(rawAt(rolledBucket.plusSeconds(40)))
        assertTrue(rawAt(neverRolledBucket.plusSeconds(10)), "평균이 없는 구간의 원본을 지웠다")
        assertTrue(rawAt(lateBucket.plusSeconds(10)), "늦게 들어온 원본이 있는 구간의 앞선 원본을 지웠다")
        assertTrue(rawAt(lateBucket.plusSeconds(70)), "평균보다 늦게 들어온 원본을 지웠다")
        assertEquals(1, rollupsOf(rolledBucket, rolledBucket.plus(Duration.ofMinutes(5))).size, "원본을 지우며 평균도 지웠다")

        // 다음 바퀴가 다시 계산하면 그 구간들도 지울 수 있다
        store.rollUp(firstRun.plus(Duration.ofMinutes(1)))
        assertEquals(MetricDeletion(3, 0), store.deleteExpired(now))
        assertFalse(rawAt(neverRolledBucket.plusSeconds(10)))
        assertFalse(rawAt(lateBucket.plusSeconds(70)))
        assertEquals(MetricAggregate(10.0, 10.0, 2), rollupsOf(lateBucket, lateBucket.plus(Duration.ofMinutes(5))).single().values["cpu_pct"])
        assertEquals(MetricDeletion(0, 0), store.deleteExpired(now), "두 자리가 함께 돌아도 결과가 같다")
    }

    @Test
    fun `원본 7일의 경계 — 정확히 7일 된 것은 남고 1ms 더 오래된 것은 지운다`() {
        val boundary = now.minus(Duration.ofDays(7))
        store.insertAll(listOf(sample(boundary.minusMillis(1)), sample(boundary), sample(boundary.plusMillis(1))))
        store.rollUp(now)
        assertEquals(MetricDeletion(1, 0), store.deleteExpired(now))
        assertFalse(rawAt(boundary.minusMillis(1)))
        assertTrue(rawAt(boundary))
        assertTrue(rawAt(boundary.plusMillis(1)))
    }

    @Test
    fun `값이 모두 null 인 구간도 평균을 만든 뒤에는 지울 수 있다`() {
        val old = now.minus(Duration.ofDays(9))
        store.insertAll(listOf(sample(old.plusSeconds(3), host = null, missing = listOf("host"))))
        store.rollUp(now)
        assertEquals(MetricDeletion(1, 0), store.deleteExpired(now), "평균에 값이 하나도 없다고 원본을 영영 남겼다")
    }

    @Test
    fun `5분 평균은 30일 뒤 지운다 — 구간 시작이 정확히 30일 전인 것은 남는다`() {
        val boundary = now.minus(Duration.ofDays(30))
        val older = boundary.minus(Duration.ofMinutes(5))
        store.insertAll(listOf(sample(older.plusSeconds(1)), sample(boundary.plusSeconds(1)), sample(now.minus(Duration.ofDays(2)))))
        store.rollUp(now)
        assertEquals(MetricDeletion(2, 1), store.deleteExpired(now))
        assertEquals(emptyList<Any>(), rollupsOf(older, boundary))
        assertEquals(1, rollupsOf(boundary, boundary.plus(Duration.ofMinutes(5))).size)
        assertTrue(rawAt(now.minus(Duration.ofDays(2))))
    }

    // ── 조회(경계를 실제 저장소로 거쳐) ───────────────────

    private val controller = MetricController(store, "secret", "ci-agent-164")
    private fun request() = MockHttpServletRequest().apply { addHeader("X-Observe-Secret", "secret") }

    @Suppress("UNCHECKED_CAST")
    private fun data(response: ResponseEntity<out Any>): Map<String, Any?> {
        assertEquals(200, response.statusCode.value(), "${response.body}")
        return (response.body as Map<String, Any?>)["data"] as Map<String, Any?>
    }

    @Suppress("UNCHECKED_CAST")
    private fun points(data: Map<String, Any?>) = data["points"] as List<Map<String, Any?>>

    private fun series(from: Instant, to: Instant, scope: String = "host", name: String? = null, server: String = "ci-agent-158", at: Instant = now) =
        data(controller.series(request(), server, scope, name, from.toString(), to.toString(), at))

    private fun number(point: Map<String, Any?>, field: String) = (point[field] as Number?)?.toDouble()

    @Test
    fun `원본 조회 — 30초 간격, 값이 없는 구간은 점이 없고 점의 시각은 from 에서 센 칸의 시작이다`() {
        val from = now.minus(Duration.ofHours(1))
        store.insertAll(listOf(
            sample(from.plus(Duration.ofMinutes(10)), host = cpu(10.0)),
            sample(from.plus(Duration.ofMinutes(10)).plusSeconds(31), host = cpu(20.0)),
            sample(from.plus(Duration.ofMinutes(30)).plusSeconds(5), host = cpu(30.0)),
        ))
        val body = series(from, now)
        assertEquals(30_000L, (body["step_ms"] as Number).toLong())
        assertEquals("raw", body["source"])
        val found = points(body)
        assertEquals(listOf(from.plus(Duration.ofMinutes(10)), from.plus(Duration.ofMinutes(10)).plusSeconds(30), from.plus(Duration.ofMinutes(30))).map { it.toString() },
            found.map { it["at"] }, "0 으로 채운 점이 있거나 칸의 시작이 아니다")
        assertEquals(listOf(10.0, 20.0, 30.0), found.map { number(it, "cpu_pct") })
    }

    @Test
    fun `큰 간격으로 묶으면 평균과 최댓값이고 null 은 빠진다`() {
        val from = now.minus(Duration.ofDays(2))                                    // 2일 ÷ 500 = 345.6초 → 360초
        val cell = from.plus(Duration.ofMinutes(60))
        store.insertAll(listOf(sample(cell.plusSeconds(1), host = cpu(10.0)), sample(cell.plusSeconds(31), host = cpu(30.0)), sample(cell.plusSeconds(61), host = cpu(null))))
        val body = series(from, now)
        assertEquals(360_000L, (body["step_ms"] as Number).toLong())
        val point = points(body).single()
        assertEquals(cell.toString(), point["at"])
        assertEquals(20.0, number(point, "cpu_pct"))
        assertEquals(30.0, number(point, "cpu_pct_max"))
        assertEquals(0.5, number(point, "load1"))
        assertNull(point["mem_used"])
    }

    @Test
    fun `점은 500 개를 넘지 않는다 — 500 칸 기간에 501 개의 표본이 있어도`() {
        val from = now.minusSeconds(500L * 30)
        store.insertAll((0..500).map { sample(from.plusSeconds(it * 30L), host = cpu(it.toDouble() % 100)) })   // 마지막 하나는 to 에 딱 맞는다
        val body = series(from, now)
        assertEquals(30_000L, (body["step_ms"] as Number).toLong())
        assertEquals(500, points(body).size)
        val wider = series(from.minusSeconds(30), now)                               // 501 칸 → 60초
        assertEquals(60_000L, (wider["step_ms"] as Number).toLong())
        assertTrue(points(wider).size <= 500, "${points(wider).size}")
    }

    @Test
    fun `7일보다 오래된 구간은 5분 평균에서 읽고 큰 간격은 표본 수로 가중한 평균과 최댓값의 최댓값이다`() {
        val from = now.minus(Duration.ofDays(20))                                   // 10일 ÷ 500 = 1,728초 → 1,740초
        val to = now.minus(Duration.ofDays(10))
        store.insertAll(listOf(
            sample(from.plusSeconds(10), host = cpu(10.0)),                           // 첫 5분: 평균 10, 표본 1
            sample(from.plus(Duration.ofMinutes(10)).plusSeconds(10), host = cpu(30.0)),   // 세 번째 5분: 평균 40, 최댓값 50, 표본 3
            sample(from.plus(Duration.ofMinutes(10)).plusSeconds(40), host = cpu(40.0)),
            sample(from.plus(Duration.ofMinutes(10)).plusSeconds(70), host = cpu(50.0)),
        ))
        store.rollUp(now)
        assertEquals(MetricDeletion(4, 0), store.deleteExpired(now), "원본이 지워져야 평균에서 읽는 것을 확인할 수 있다")
        val body = series(from, to)
        assertEquals("rollup", body["source"])
        assertEquals(1_740_000L, (body["step_ms"] as Number).toLong())
        val point = points(body).single()
        assertEquals(from.toString(), point["at"])
        assertEquals(32.5, number(point, "cpu_pct"), "평균의 단순 평균(25)이 아니라 표본 수 가중 평균이어야 한다")
        assertEquals(50.0, number(point, "cpu_pct_max"))
    }

    @Test
    fun `7일 경계를 걸치면 mixed — 오래된 쪽은 평균, 최근 쪽은 원본에서 읽는다`() {
        val from = now.minus(Duration.ofDays(8))
        val to = now.minus(Duration.ofDays(6))                                      // 2일 → 360초
        val oldCell = now.minus(Duration.ofDays(7)).minus(Duration.ofHours(1))
        val newCell = now.minus(Duration.ofDays(6)).minus(Duration.ofHours(1))
        store.insertAll(listOf(
            sample(oldCell.plusSeconds(5), host = cpu(10.0)), sample(oldCell.plusSeconds(35), host = cpu(30.0)),
            sample(newCell.plusSeconds(5), host = cpu(70.0)),
        ))
        store.rollUp(now)
        // 평균을 만든 뒤 오래된 쪽에 원본이 하나 더 들어왔다(아직 다시 계산 전) — 오래된 쪽은 원본이 아니라 평균에서 읽어야 한다
        store.insertAll(listOf(sample(oldCell.plusSeconds(65), receivedAt = now.plusSeconds(1), host = cpu(90.0))))
        val body = series(from, to)
        assertEquals("mixed", body["source"])
        assertEquals(360_000L, (body["step_ms"] as Number).toLong())
        val found = points(body)
        assertEquals(2, found.size, "$found")
        assertEquals(20.0, number(found[0], "cpu_pct"), "오래된 쪽을 원본(10·30·90)에서 읽었거나 원본과 평균을 함께 셌다")
        assertEquals(30.0, number(found[0], "cpu_pct_max"))
        assertEquals(70.0, number(found[1], "cpu_pct"))
    }

    @Test
    fun `컨테이너는 자리 이름을 뗀 이름으로 묻고 같은 시각의 두 자리는 합, 다른 서버와 다른 이름은 섞이지 않는다`() {
        val from = now.minus(Duration.ofHours(1))
        val first = from.plus(Duration.ofMinutes(5))
        fun container(name: String, cpu: Double, memory: Long) = ContainerValues(name, "0123456789ab", "study-note-deploy-system-front", cpu, memory, 0, "running")
        store.insertAll(listOf(
            sample(first, containers = listOf(container("front-app-blue", 10.0, 100), container("front-app-green", 5.0, 50), container("front-app-worker", 99.0, 999))),
            sample(first.plusSeconds(30), containers = listOf(container("front-app-green", 20.0, 200))),
            sample(first, server = "ci-agent-164", containers = listOf(container("front-app-green", 70.0, 700))),
        ))
        val found = points(series(from, now, "container", "front-app"))
        assertEquals(listOf(first.toString(), first.plusSeconds(30).toString()), found.map { it["at"] })
        assertEquals(15.0, number(found[0], "cpu_pct"), "전환 중 두 자리는 합")
        assertEquals(150.0, number(found[0], "mem_used"))
        assertEquals(20.0, number(found[1], "cpu_pct"))

        val wide = points(series(now.minus(Duration.ofDays(2)), now, "container", "front-app")).single()   // 360초 칸 하나에 두 표본
        assertEquals(17.5, number(wide, "cpu_pct"), "칸 안의 표본은 시각마다 합한 뒤 평균")
        assertEquals(20.0, number(wide, "cpu_pct_max"))
    }

    @Test
    fun `GPU 는 번호로 묻는다`() {
        val from = now.minus(Duration.ofHours(1))
        store.insertAll(listOf(sample(from.plusSeconds(60), gpus = listOf(GpuValues(0, 10.0, 1, 2, 30.0), GpuValues(1, 90.0, 1, 2, 70.0)))))
        assertEquals(10.0, number(points(series(from, now, "gpu", "0")).single(), "util_pct"))
        assertEquals(90.0, number(points(series(from, now, "gpu", "1")).single(), "util_pct"))
        assertEquals(emptyList<Any>(), points(series(from, now, "gpu", "2")))
    }

    @Test
    fun `저장소의 계열 조회는 칸 시작과 묶음을 돌려준다`() {
        val from = now.minus(Duration.ofHours(1))
        store.insertAll(listOf(sample(from.plusSeconds(61), host = cpu(4.0)), sample(from.plusSeconds(75), host = cpu(8.0))))
        val found = store.series(SeriesQuery("ci-agent-158", MetricScope.HOST, null, from, now, Duration.ofSeconds(30), now.minus(Duration.ofDays(7))))
        assertEquals(from.plusSeconds(60), found.single().at)
        assertEquals(MetricAggregate(6.0, 8.0, 2), found.single().values["cpu_pct"])
    }

    @Test
    fun `latest 는 서버마다 가장 최근 값과 수신 시각·stale 을 내고 기대 서버 중 안 보낸 서버는 수신 없음이며 firing 은 빈 배열`() {
        store.insertAll(listOf(
            sample(now.minusSeconds(70), receivedAt = now.minusSeconds(69), server = "ci-agent-158", host = cpu(5.0)),
            sample(now.minusSeconds(40), receivedAt = now.minusSeconds(39), server = "ci-agent-158", host = cpu(7.0)),
            sample(now.minus(Duration.ofMinutes(4)), receivedAt = now.minus(Duration.ofMinutes(4)), server = "ci-agent-9", host = cpu(9.0)),
        ))
        val body = data(controller.latest(request(), now))
        assertEquals(emptyList<Any>(), body["firing"])
        @Suppress("UNCHECKED_CAST")
        val servers = (body["servers"] as List<Map<String, Any?>>).associateBy { it["server"] }
        assertEquals(setOf("ci-agent-158", "ci-agent-164", "ci-agent-9"), servers.keys)
        val fresh = servers["ci-agent-158"]!!
        assertEquals(now.minusSeconds(40).toString(), fresh["at"])
        assertEquals(now.minusSeconds(39).toString(), fresh["received_at"])
        assertEquals(39_000L, (fresh["age_ms"] as Number).toLong())
        assertEquals(false, fresh["stale"])
        @Suppress("UNCHECKED_CAST")
        assertEquals(7.0, ((fresh["host"] as Map<String, Any?>)["cpu_pct"] as Number).toDouble())
        assertEquals(true, servers["ci-agent-9"]!!["stale"])
        assertEquals(null to true, servers["ci-agent-164"]!!.let { it["at"] to it["stale"] }, "기대 서버의 수신 없음")
    }

    @Test
    fun `containers 는 최근 24시간에 보인 계열 이름과 마지막 시각, at 을 주면 앞뒤 5분(양 끝 포함)에 보인 것만`() {
        fun container(name: String) = ContainerValues(name, "0123456789ab", "study-note-deploy-system-front", 1.0, 1, 0, "running")
        val at = now.minus(Duration.ofHours(3))
        store.insertAll(listOf(
            sample(at.minus(Duration.ofMinutes(5)), containers = listOf(container("front-app-blue"))),
            sample(at.plus(Duration.ofMinutes(5)), containers = listOf(container("backend-app-green"))),
            sample(at.plus(Duration.ofMinutes(5)).plusMillis(1), containers = listOf(container("llm-app-blue"))),
            sample(now.minus(Duration.ofMinutes(10)), containers = listOf(container("front-app-green"))),
            sample(now.minus(Duration.ofHours(25)), containers = listOf(container("old-app-blue"))),
            sample(now.minus(Duration.ofMinutes(1)), server = "ci-agent-164", containers = listOf(container("other-app"))),
        ))
        @Suppress("UNCHECKED_CAST")
        val recent = (data(controller.containers(request(), "ci-agent-158", null, now))["containers"] as List<Map<String, Any?>>)
        assertEquals(listOf("backend-app", "front-app", "llm-app", "probe-app"), recent.map { it["name"] })
        assertEquals(now.minus(Duration.ofMinutes(10)).toString(), recent[1]["last_seen_at"], "마지막으로 보인 시각")

        @Suppress("UNCHECKED_CAST")
        val around = (data(controller.containers(request(), "ci-agent-158", at.toString(), now))["containers"] as List<Map<String, Any?>>)
        assertEquals(listOf("backend-app", "front-app", "probe-app"), around.map { it["name"] })
        assertEquals(at.minus(Duration.ofMinutes(5)).toString(), around[1]["last_seen_at"], "그 창 안에서 마지막으로 보인 시각")
    }
}
