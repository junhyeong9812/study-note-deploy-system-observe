package xyz.junproject.observe

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.convert.DurationStyle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import xyz.junproject.observe.auth.AuthAttemptPolicy
import xyz.junproject.observe.auth.AuthAttemptStore
import xyz.junproject.observe.auth.SchemaOnFirstUseAuthAttempts
import xyz.junproject.observe.collect.CollectorHealth
import xyz.junproject.observe.collect.LogCollector
import xyz.junproject.observe.collect.RedisLogStreamSource
import xyz.junproject.observe.log.LogStore
import xyz.junproject.observe.metric.MetricCollector
import xyz.junproject.observe.metric.MetricController
import xyz.junproject.observe.metric.MetricHealth
import xyz.junproject.observe.metric.MetricStore
import xyz.junproject.observe.metric.RedisMetricStreamSource
import xyz.junproject.observe.threshold.AlertDispatcher
import xyz.junproject.observe.threshold.HttpAlertSender
import xyz.junproject.observe.threshold.ResourceThresholds
import xyz.junproject.observe.threshold.ThresholdJudge
import xyz.junproject.observe.threshold.ThresholdRound
import xyz.junproject.observe.threshold.ThresholdStore
import org.springframework.core.env.Environment
import java.time.Duration
import kotlin.reflect.full.primaryConstructor
import jakarta.annotation.PreDestroy
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import xyz.junproject.observe.threshold.SilentGate

/** 조립 — 연결 정보는 환경변수에서. DB 비밀번호 같은 값은 로그에 내지 않는다. */
@Configuration
class ObserveConfiguration {
    private val log = LoggerFactory.getLogger("observe")

    /** 이 설정이 만든 연결 풀 — 문맥이 닫힐 때 함께 닫는다([closePools]) */
    private val pools = CopyOnWriteArrayList<HikariDataSource>()

    private fun pool(name: String, url: String, user: String, password: String, size: Int, statementTimeoutMs: Int, extraInitSql: String = "",
                     connectionTimeoutMs: Long = 5_000, validationTimeoutMs: Long = 5_000) =
        HikariDataSource(HikariConfig().apply {
            poolName = name
            jdbcUrl = url
            username = user
            this.password = password
            maximumPoolSize = size
            connectionTimeout = connectionTimeoutMs
            validationTimeout = validationTimeoutMs
            // 뜰 때 DB 가 아직 준비 전이어도 죽지 않는다 — 수집 바퀴가 실패로 남기고 다시 시도한다
            initializationFailTimeout = -1
            connectionInitSql = "SET statement_timeout = $statementTimeoutMs$extraInitSql"
        }).also { pools += it }

    /**
     * 문맥이 닫힐 때 풀을 닫는다 — 풀은 빈이 아니라 빈 메서드 안에서 만들어져 스프링이 닫아 주지 않는다(닫지 않으면 문맥마다 연결이 남는다).
     * 이 설정 빈은 그 메서드들이 만든 빈(저장소 · 수집기 · 예약)보다 늦게 없어지므로 수집 스레드가 멈춘 뒤에 닫힌다.
     */
    @PreDestroy
    fun closePools() {
        for (pool in pools) runCatching { pool.close() }.onFailure { log.warn("observe:observe:pool close failed: {}", it.javaClass.simpleName) }
        pools.clear()
    }

    /**
     * 수집·조회·헬스의 연결 풀을 나눈다 — 무거운 조회가 연결을 다 잡아도, 인증 없는 헬스 요청이 몰려도 수집은 밀리지 않는다.
     * 조회는 10초에서 끊는다. 헬스는 1개·짧은 대기 — 몰리면 헬스끼리만 기다리다 503 이 되고, DB 가 끊기면 몇 초 안에 503 이 된다.
     */
    @Bean
    fun logStore(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
    ): LogStore {
        val ingest = pool("observe-ingest", url, user, password, size = 2, statementTimeoutMs = 30_000)
        // 조회는 늘 값을 보고 계획한다 — 검색어(q)가 자리표시자라 같은 문장이 되풀이되면 PostgreSQL 이 값과 무관한 일반 계획으로
        // 바꿀 수 있는데, 그 계획으로는 흔한 말 검색이 75만 행에서 2초 걸렸다(값을 보고 세운 계획은 1ms). 계획 비용은 1ms 안팎.
        val query = pool("observe-query", url, user, password, size = 4, statementTimeoutMs = 10_000,
            extraInitSql = "; SET plan_cache_mode = force_custom_plan")
        val health = pool("observe-health", url, user, password, size = 1, statementTimeoutMs = 2_000, connectionTimeoutMs = 2_000, validationTimeoutMs = 1_000)
        return LogStore(JdbcTemplate(ingest), JdbcTemplate(query), JdbcTemplate(health))
    }

    /**
     * 예약 실행기 — 스레드 넷(5분 평균 · 판정 · 보내기 · 하루 한 번 삭제가 서로 기다리지 않게, §15.1). 스프링 부트의 기본(가상 스레드 설정일 때의
     * 실행기)은 고정 지연 작업을 한 줄로 돌려, 알림 한 건에 3초 걸리는 동안 판정이 멈췄다(실측 — `AlertDispatchScheduleEngineTest`).
     * 이 빈이 있으면 부트의 기본 실행기는 만들어지지 않는다. 닫힐 때 도는 작업을 기다리지 않는다(기존과 같다).
     */
    @Bean
    fun taskScheduler() = ThreadPoolTaskScheduler().apply {
        poolSize = 4
        setThreadNamePrefix("observe-schedule-")
    }

    /** 잠금 수치 — 기본값이 명세(5회·15분·90일). 시간은 `15m`·`90d` 꼴 */
    @Bean
    fun authAttemptPolicy(
        @Value("\${observe.auth.failure-limit:5}") failureLimit: Int,
        @Value("\${observe.auth.lock-duration:15m}") lockDuration: String,
        @Value("\${observe.auth.retention:90d}") retention: String,
    ) = AuthAttemptPolicy(failureLimit, DurationStyle.detectAndParse(lockDuration), DurationStyle.detectAndParse(retention))

    /**
     * 로그인 시도 저장·잠금 판정 — 전용 연결 풀(3개). 수집 풀(2개)을 같이 쓰면 판정이 몰릴 때 로그 저장이 밀리고,
     * 조회 풀(4개·10초)을 같이 쓰면 무거운 로그 검색 뒤에서 로그인이 기다린다. 판정은 상태 행에서 한 줄로 서므로 연결이 많아도 빨라지지 않는다.
     * 문장은 4초에서 끊는다 — 어드민이 5초에 포기하기 전에 답이 가게(포기하면 같은 attempt_id 로 다시 묻는다).
     * 스키마는 처음 닿을 때 만든다([SchemaOnFirstUseAuthAttempts]).
     */
    @Bean
    fun authAttempts(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
        policy: AuthAttemptPolicy,
    ): SchemaOnFirstUseAuthAttempts {
        val auth = pool("observe-auth", url, user, password, size = 3, statementTimeoutMs = 4_000, connectionTimeoutMs = 3_000, validationTimeoutMs = 1_000)
        return SchemaOnFirstUseAuthAttempts(AuthAttemptStore(JdbcTemplate(auth), policy))
    }

    /** 뜬 직후 로그인 시도 표를 미리 만든다(최선 노력 — 실패해도 앱은 뜨고 첫 호출이 다시 만든다) */
    @EventListener(ApplicationReadyEvent::class)
    fun prepareAuthAttemptSchema(event: ApplicationReadyEvent) {
        event.applicationContext.getBean(SchemaOnFirstUseAuthAttempts::class.java).prepare()
    }

    /**
     * 지표 저장소 — 로그와 같은 DB, 연결 풀은 지표 전용(수집 2 · 조회 2 · 헬스의 상태 읽기 1 · 헬스의 선택 칸 읽기 1). 지표가 무겁거나 실패해도
     * 로그 수집·조회의 풀을 잡지 않는다. 5분 평균 · 보관 삭제도 수집 풀을 쓴다(구간마다 원본 저장이 잠시 기다린다 — [MetricStore.rollUpBuckets]).
     * 헬스의 두 풀은 연결 하나 · 연결 대기 2초 · 문장 2초 — 선택 칸의 표가 막혀 버려진 읽기가 연결을 쥐어도 상태 읽기는 기다리지 않는다(§16.1).
     * 판정의 읽기는 판정 · 알림 풀([thresholdJdbc])로 — 어드민 조회가 조회 풀을 다 잡아도 판정이 기다리지 않는다(§16).
     */
    @Bean
    fun metricStore(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
    ): MetricStore {
        val ingest = pool("observe-metric-ingest", url, user, password, size = 2, statementTimeoutMs = 30_000)
        val query = pool("observe-metric-query", url, user, password, size = 2, statementTimeoutMs = 10_000)
        val health = pool("observe-metric-health", url, user, password, size = 1, statementTimeoutMs = 2_000, connectionTimeoutMs = 2_000, validationTimeoutMs = 1_000)
        val healthDetail = pool("observe-metric-health-detail", url, user, password, size = 1, statementTimeoutMs = 2_000, connectionTimeoutMs = 2_000,
            validationTimeoutMs = 1_000)
        return MetricStore(JdbcTemplate(ingest), JdbcTemplate(query), JdbcTemplate(health), judge = thresholdJdbc(url, user, password),
            healthDetail = JdbcTemplate(healthDetail))
    }

    private var sharedThresholdJdbc: JdbcTemplate? = null

    /**
     * 판정 · 알림 풀(5개 · 문장 10초) — 판정 한 바퀴(잠금 트랜잭션 1 + 표본 읽기 1) · 보내기(잠금 1 + 표시 1) · firing 이 함께 쓴다.
     * 지표 저장소(판정의 읽기)와 판정 · 알림 저장소가 같은 풀을 나눠 쓴다.
     */
    @Synchronized
    private fun thresholdJdbc(url: String, user: String, password: String): JdbcTemplate = sharedThresholdJdbc
        ?: JdbcTemplate(pool("observe-threshold", url, user, password, size = 5, statementTimeoutMs = 10_000)).also { sharedThresholdJdbc = it }

    /** 뜬 직후 지표 표를 미리 만든다(최선 노력 — 실패해도 앱은 뜨고 수집 바퀴 · 첫 조회가 다시 만든다) */
    @EventListener(ApplicationReadyEvent::class)
    fun prepareMetricSchema(event: ApplicationReadyEvent) {
        try {
            event.applicationContext.getBean(MetricStore::class.java).prepareSchema()
            log.info("observe:observe:metric schema ready")
        } catch (error: Exception) {
            log.error("observe:observe:metric schema failed — will retry on first use: {}: {}", error.javaClass.simpleName, error.message?.take(200))
        }
    }

    @Bean
    fun metricStreamSource(
        redis: StringRedisTemplate,
        @Value("\${observe.metrics.stream:metrics}") stream: String,
        @Value("\${observe.metrics.group:observe}") group: String,
    ): RedisMetricStreamSource {
        log.info("observe:observe:collecting metric stream '{}' as group '{}'", stream, group)
        return RedisMetricStreamSource(redis, stream, group)
    }

    @Bean
    fun metricCollector(source: RedisMetricStreamSource, store: MetricStore) = MetricCollector(source, store)

    @Bean
    fun metricHealth(collector: MetricCollector, source: RedisMetricStreamSource, store: MetricStore, schedule: ObserveSchedule, thresholdRound: ThresholdRound) =
        MetricHealth(collector, { source.unacknowledgedCount() }, { source.lag() }, { store.latestStoredAt() }, { store.lastReceivedByServer() },
            { schedule.lastMetricRollup }, { schedule.lastMetricRollupSuccessAt }, { schedule.lastMetricRetention }, thresholds = thresholdRound,
            latestSamples = { store.latestHeads(Instant.now().minus(MetricController.LATEST_WINDOW)) })

    /**
     * 임계값 수치 — 칸마다 설정 키 `observe.metrics.alert.<케밥꼴>` 또는 환경 변수 `METRICS_ALERT_<대문자 밑줄꼴>`(앞이 우선), 없으면
     * [ResourceThresholds] 의 기본값(명세). 시간은 `5m` · `1h` 꼴. 말이 안 되는 값이면 뜰 때 실패한다.
     */
    @Bean
    fun resourceThresholds(environment: Environment): ResourceThresholds {
        val constructor = requireNotNull(ResourceThresholds::class.primaryConstructor)
        val given = constructor.parameters.mapNotNull { parameter ->
            val name = requireNotNull(parameter.name)
            val words = name.split(Regex("(?=[A-Z])")).map { it.lowercase() }
            val text = environment.getProperty("observe.metrics.alert.${words.joinToString("-")}")
                ?: environment.getProperty("METRICS_ALERT_${words.joinToString("_").uppercase()}")
                ?: return@mapNotNull null
            val value: Any = when (parameter.type.classifier) {
                Double::class -> text.trim().toDouble()
                Int::class -> text.trim().toInt()
                Duration::class -> DurationStyle.detectAndParse(text.trim())
                else -> throw IllegalStateException("알 수 없는 임계값 칸: $name")
            }
            parameter to value
        }.toMap()
        return constructor.callBy(given).also { log.info("observe:observe:resource thresholds {}", it) }
    }

    /** 판정 · 알림 표 — 판정 · 알림 풀([thresholdJdbc]). 헬스는 이 풀을 쓰지 않는다(바퀴가 세어 둔 건수만) */
    @Bean
    fun thresholdStore(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
    ) = ThresholdStore(thresholdJdbc(url, user, password))

    /** 뜬 직후 판정 · 알림 표를 미리 만든다(최선 노력 — 실패해도 첫 바퀴 · 첫 조회가 다시 만든다) */
    @EventListener(ApplicationReadyEvent::class)
    fun prepareThresholdSchema(event: ApplicationReadyEvent) {
        try {
            event.applicationContext.getBean(ThresholdStore::class.java).prepareSchema()
        } catch (error: Exception) {
            log.error("observe:observe:threshold schema failed — will retry on first use: {}: {}", error.javaClass.simpleName, error.message?.take(200))
        }
    }

    /** 알림 서버 `POST /events` — 주소 · 비밀값은 로그에 내지 않는다(주소가 있는지만) */
    @Bean
    fun httpAlertSender(
        @Value("\${observe.alert.events-url:}") eventsUrl: String,
        @Value("\${observe.alert.secret:}") secret: String,
        @Value("\${observe.alert.timeout-ms:3000}") timeoutMs: Long,
    ): HttpAlertSender {
        if (eventsUrl.isBlank()) log.warn("observe:observe:alert events url is not configured — alerts stay undelivered")
        return HttpAlertSender(eventsUrl, secret, Duration.ofMillis(timeoutMs))
    }

    @Bean
    fun alertDispatcher(store: ThresholdStore, sender: HttpAlertSender, @Value("\${observe.alert.timeout-ms:3000}") timeoutMs: Long) =
        AlertDispatcher(store, sender, sendTimeout = Duration.ofMillis(timeoutMs))   // 한 바퀴는 "쓴 시간 + 한 건의 기한"이 30초를 넘게 되면 멈춘다

    @Bean
    fun thresholdRound(
        metricStore: MetricStore, store: ThresholdStore, thresholds: ResourceThresholds, dispatcher: AlertDispatcher, collector: MetricCollector,
        @Value("\${observe.metrics.expected-servers:}") expectedServers: String,
        @Value("\${observe.metrics.alert.silent-warm-up:3m}") silentWarmUp: String,
        @Value("\${observe.metrics.alert.silent-give-up-after:15m}") silentGiveUpAfter: String,
    ): ThresholdRound {
        // 끊김은 이 프로세스가 뜬 지 warm-up · 수집이 한 번 따라잡은 뒤부터(따라잡지 못해도 give-up 뒤) — 꺼져 있던 동안을 "모두 끊김"으로 알리지 않게.
        // 말이 안 되는 값이면 뜰 때 실패한다
        val warmUp = DurationStyle.detectAndParse(silentWarmUp.trim())
        val giveUpAfter = DurationStyle.detectAndParse(silentGiveUpAfter.trim())
        require(!warmUp.isNegative && !warmUp.isZero) { "observe.metrics.alert.silent-warm-up 은 0 보다 길어야 한다" }
        require(!giveUpAfter.isNegative && !giveUpAfter.isZero) { "observe.metrics.alert.silent-give-up-after 는 0 보다 길어야 한다" }
        require(giveUpAfter >= warmUp) { "observe.metrics.alert.silent-give-up-after 는 silent-warm-up 보다 짧을 수 없다" }
        return ThresholdRound(metricStore, store, ThresholdJudge(thresholds), dispatcher, store,
            expectedServers.split(',').map { it.trim() }.filter { it.isNotEmpty() }.toSet(),
            silentGate = SilentGate(Instant.now(), { collector.caughtUpAt }, warmUp, giveUpAfter))
    }

    @Bean
    fun logStreamSource(
        redis: StringRedisTemplate,
        @Value("\${observe.stream:logs}") stream: String,
        @Value("\${observe.group:observe}") group: String,
    ): RedisLogStreamSource {
        log.info("observe:observe:collecting stream '{}' as group '{}'", stream, group)
        return RedisLogStreamSource(redis, stream, group)
    }

    @Bean
    fun logCollector(source: RedisLogStreamSource, store: LogStore) = LogCollector(source, store)

    @Bean
    fun collectorHealth(collector: LogCollector, source: RedisLogStreamSource, store: LogStore, schedule: ObserveSchedule, metricHealth: MetricHealth) =
        CollectorHealth(collector, { source.unacknowledgedCount() }, { source.lag() }, { store.latestStoredAt() }, { schedule.lastRetention },
            lastAuthRetention = { schedule.lastAuthRetention }, metrics = metricHealth)
}
