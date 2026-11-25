package xyz.junproject.observe.visit

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import xyz.junproject.observe.log.StreamId
import xyz.junproject.observe.metric.MetricStreamRecord

/**
 * 테스트용 방문 항목 — front 가 `XADD visits` 로 보내는 칸 그대로(계약: [VisitEntryParser] 의 칸 표).
 * 주소는 문서용 대역(203.0.113.0/24 · 2001:db8::/32)의 알아볼 수 있는 값 — 로그 · 응답 · 헬스에 이 글자가 없는지 찾는다.
 */
object VisitRecords {
    const val ADDRESS_V4 = "203.0.113.77"
    const val ADDRESS_V6 = "2001:db8::77"
    /** 그날의 구분값(SHA-256 16진 64자) — 응답에 이 글자가 없는지 찾는다 */
    val VISITOR_ONE = "c0ffee01" + "a".repeat(56)
    val VISITOR_TWO = "c0ffee02" + "b".repeat(56)
    /** 경로에 붙어 온 검색어 — 저장 · 로그 · 응답 어디에도 남지 않아야 한다 */
    const val SEARCH_WORDS = "secretsearchword"

    fun fields(
        path: String = "/wiki/kubernetes/pod",
        screen: String = "topic",
        referrerHost: String? = "www.google.com",
        device: String = "desktop",
        visitor: String = VISITOR_ONE,
        ip: String = ADDRESS_V4,
        version: String = "1",
    ): Map<String, String> = buildMap {
        put("v", version)
        put("path", path)
        put("screen", screen)
        if (referrerHost != null) put("referrer_host", referrerHost)
        put("device", device)
        put("visitor", visitor)
        put("ip", ip)
    }

    fun record(id: String, fields: Map<String, String> = fields()) = MetricStreamRecord(id, fields)

    fun visit(
        millis: Long,
        sequence: Int = 0,
        path: String = "/wiki/kubernetes/pod",
        screen: String = "topic",
        referrerHost: String? = "www.google.com",
        device: VisitDevice = VisitDevice.DESKTOP,
        visitor: String = VISITOR_ONE,
        ip: String = ADDRESS_V4,
    ) = Visit(StreamId(millis, sequence), path, screen, referrerHost, device, visitor, ip)
}

/**
 * 메모리 저장소 — 같은 스트림 ID 는 한 번만(새로 넣은 ID 만 돌려준다). [failure] 가 있으면 저장이 그 예외로 실패한다.
 * [resultUnknownFor] 에 든 ID 는 드라이버가 "결과 모름"을 돌려준 것처럼 — 넣었는지와 무관하게 [VisitInsertion.resultUnknown] 으로 돌려준다.
 */
class FakeVisitSink(private val events: MutableList<String> = mutableListOf()) : VisitSink {
    val stored = LinkedHashMap<StreamId, Visit>()
    var failure: RuntimeException? = null
    var schemaFailing = false
    var schemaCreations = 0

    override fun createSchema() {
        if (schemaFailing) throw IllegalStateException("permission denied for schema public")
        schemaCreations++
    }

    var resultUnknownFor: Set<StreamId> = emptySet()

    override fun insertAll(visits: List<Visit>): VisitInsertion {
        failure?.let { events += "insert-failed"; throw it }
        visits.forEach { events += "insert:${it.id}" }
        val inserted = visits.filter { stored.putIfAbsent(it.id, it) == null }.map { it.id }.toSet()
        val unknown = visits.map { it.id }.filter { it in resultUnknownFor }.toSet()
        return VisitInsertion(inserted - unknown, unknown)
    }
}

/** 애플리케이션 로그 전체(루트 로거)를 모은다 — 닫으면 뗀다 */
class CapturedLogs : AutoCloseable {
    private val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) as Logger
    private val appender = ListAppender<ILoggingEvent>().also { it.start(); root.addAppender(it) }

    /** 모인 줄 전부 — 문구와 예외(원인 사슬 포함)의 글까지 */
    fun text(): String = synchronized(appender) { appender.list.toList() }.joinToString("\n") { event ->
        val causes = generateSequence(event.throwableProxy) { it.cause }.joinToString("\n") { "${it.className}: ${it.message}" }
        "${event.formattedMessage}\n$causes"
    }

    fun lines(): List<String> = synchronized(appender) { appender.list.toList() }.map { it.formattedMessage }

    /** ERROR 수준의 줄만 — 문구와 예외(원인 사슬 포함)의 글까지 */
    fun errorLines(): List<String> = synchronized(appender) { appender.list.toList() }.filter { it.level == ch.qos.logback.classic.Level.ERROR }.map { event ->
        val causes = generateSequence(event.throwableProxy) { it.cause }.joinToString(" ") { "${it.className}: ${it.message}" }
        "${event.formattedMessage} $causes".trim()
    }

    override fun close() { root.detachAppender(appender) }
}
