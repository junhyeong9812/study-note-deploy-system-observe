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

/** 조립 — 연결 정보는 환경변수에서. DB 비밀번호 같은 값은 로그에 내지 않는다. */
@Configuration
class ObserveConfiguration {
    private val log = LoggerFactory.getLogger("observe")

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
        })

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
     * 지표 저장소 — 로그와 같은 DB, 연결 풀은 지표 전용(수집 2 · 조회 2 · 헬스 1). 지표가 무겁거나 실패해도 로그 수집·조회의 풀을 잡지 않는다.
     * 5분 평균 · 보관 삭제도 수집 풀을 쓴다(평균 한 바퀴 동안 원본 저장이 잠시 기다린다 — [MetricStore.rollUp]).
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
        return MetricStore(JdbcTemplate(ingest), JdbcTemplate(query), JdbcTemplate(health))
    }

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
            { schedule.lastMetricRollup }, { schedule.lastMetricRollupSuccessAt }, { schedule.lastMetricRetention }, thresholds = thresholdRound)

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

    /** 판정 · 알림 표 — 전용 연결 풀(4개 · 문장 10초): 판정 한 바퀴 · 보내기 잠금 · 보내기 표시 · 헬스 건수와 firing 이 함께 쓸 수 있게 */
    @Bean
    fun thresholdStore(
        @Value("\${observe.db.url}") url: String,
        @Value("\${observe.db.user}") user: String,
        @Value("\${observe.db.password}") password: String,
    ) = ThresholdStore(JdbcTemplate(pool("observe-threshold", url, user, password, size = 4, statementTimeoutMs = 10_000)))

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
    fun alertDispatcher(store: ThresholdStore, sender: HttpAlertSender) = AlertDispatcher(store, sender)

    @Bean
    fun thresholdRound(
        metricStore: MetricStore, store: ThresholdStore, thresholds: ResourceThresholds, dispatcher: AlertDispatcher,
        @Value("\${observe.metrics.expected-servers:}") expectedServers: String,
    ) = ThresholdRound(metricStore, store, ThresholdJudge(thresholds), dispatcher, store,
        expectedServers.split(',').map { it.trim() }.filter { it.isNotEmpty() }.toSet())

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
