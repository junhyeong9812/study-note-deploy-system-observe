package xyz.junproject.observe.log

import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/** 수집 쪽이 쓰는 면 */
interface LogSink {
    /** 묶음을 저장한다. 이미 있는 항목(같은 스트림 ID)은 건너뛴다. 새로 들어간 행 수를 돌려준다. 실패는 예외. */
    fun insertAll(entries: List<LogEntry>): Int
}

/** 조회 조건 — 값 검증은 API 경계에서 끝낸다 */
data class LogQuery(
    val from: Instant,
    val to: Instant,
    val service: String? = null,
    val level: String? = null,
    val requestId: String? = null,
    val text: String? = null,          // 내용 부분 일치
    val before: StreamId? = null,      // 이어 보기: 이 항목보다 오래된 것부터
    val limit: Int = 100,
)

data class LogCount(val hour: Instant, val service: String, val level: String, val count: Long)

/** 조회 쪽이 쓰는 면 */
interface LogReader {
    fun search(query: LogQuery): List<LogEntry>
    fun stats(from: Instant, to: Instant): List<LogCount>
}

/**
 * PostgreSQL 저장소. 수집과 조회가 서로 다른 연결 풀을 쓴다 — 무거운 조회가 연결을 다 잡아도 수집은 밀리지 않는다.
 */
class LogStore(private val ingest: JdbcTemplate, private val query: JdbcTemplate) : LogSink, LogReader {

    /** 스키마 — 있으면 그대로 둔다. 여러 인스턴스가 함께 떠도 안전하다. */
    fun createSchema() {
        ingest.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        ingest.execute(
            """CREATE TABLE IF NOT EXISTS log_entry (
                 stream_ms  bigint  NOT NULL,
                 stream_seq integer NOT NULL,
                 level      text    NOT NULL,
                 service    text    NOT NULL,
                 request_id text    NOT NULL,
                 message    text    NOT NULL,
                 PRIMARY KEY (stream_ms, stream_seq))"""
        )
        ingest.execute("CREATE INDEX IF NOT EXISTS log_entry_service ON log_entry (service, stream_ms DESC, stream_seq DESC)")
        ingest.execute("CREATE INDEX IF NOT EXISTS log_entry_level ON log_entry (level, stream_ms DESC, stream_seq DESC) WHERE level <> 'info'")
        ingest.execute("CREATE INDEX IF NOT EXISTS log_entry_request ON log_entry (request_id) WHERE request_id <> ''")
        ingest.execute("CREATE INDEX IF NOT EXISTS log_entry_message_trgm ON log_entry USING gin (message gin_trgm_ops)")
    }

    override fun insertAll(entries: List<LogEntry>): Int {
        if (entries.isEmpty()) return 0
        val results = ingest.batchUpdate(
            """INSERT INTO log_entry (stream_ms, stream_seq, level, service, request_id, message)
               VALUES (?,?,?,?,?,?) ON CONFLICT (stream_ms, stream_seq) DO NOTHING""",
            entries.map { arrayOf<Any>(it.id.millis, it.id.sequence, it.level, it.service, it.requestId, it.message) },
        )
        return results.count { it > 0 || it == java.sql.Statement.SUCCESS_NO_INFO }
    }

    override fun search(query: LogQuery): List<LogEntry> {
        val conditions = mutableListOf("stream_ms >= ?", "stream_ms <= ?")
        val arguments = mutableListOf<Any>(query.from.toEpochMilli(), query.to.toEpochMilli())
        query.service?.let { conditions += "service = ?"; arguments += it }
        query.level?.let { conditions += "level = ?"; arguments += it }
        query.requestId?.let { conditions += "request_id = ?"; arguments += it }
        query.text?.let { conditions += "message ILIKE ? ESCAPE '\\'"; arguments += "%${escapeLike(it)}%" }
        query.before?.let { conditions += "(stream_ms, stream_seq) < (?, ?)"; arguments += it.millis; arguments += it.sequence }
        arguments += query.limit
        return this.query.query(
            """SELECT stream_ms, stream_seq, level, service, request_id, message FROM log_entry
               WHERE ${conditions.joinToString(" AND ")}
               ORDER BY stream_ms DESC, stream_seq DESC LIMIT ?""",
            { row, _ ->
                LogEntry(StreamId(row.getLong("stream_ms"), row.getInt("stream_seq")), row.getString("level"),
                    row.getString("service"), row.getString("request_id"), row.getString("message"))
            },
            *arguments.toTypedArray(),
        )
    }

    override fun stats(from: Instant, to: Instant): List<LogCount> = query.query(
        """SELECT stream_ms / 3600000 AS hour, service, level, count(*) AS total FROM log_entry
           WHERE stream_ms >= ? AND stream_ms <= ? GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""",
        { row, _ -> LogCount(Instant.ofEpochMilli(row.getLong("hour") * 3_600_000), row.getString("service"), row.getString("level"), row.getLong("total")) },
        from.toEpochMilli(), to.toEpochMilli(),
    )

    /** [cutoff] 보다 오래된 행을 지운다. 지운 행 수를 돌려준다. */
    fun deleteOlderThan(cutoff: Instant): Int = ingest.update("DELETE FROM log_entry WHERE stream_ms < ?", cutoff.toEpochMilli())

    /** DB 에 닿는가(헬스용) — 닿지 못하면 예외 */
    fun ping() { ingest.queryForObject("SELECT 1", Int::class.java) }

    companion object {
        /** 사용자가 넣은 글자를 그대로 찾는다 — `%`·`_` 가 와일드카드로 쓰이지 않게 */
        fun escapeLike(text: String) = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    }
}
