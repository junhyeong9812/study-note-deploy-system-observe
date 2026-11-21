package xyz.junproject.observe.log

import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/** 수집 쪽이 쓰는 면 */
interface LogSink {
    /** 저장할 표를 만든다(있으면 그대로). 수집 바퀴가 첫 성공 전에 부른다 — 실패가 그 바퀴의 실패 사유로 헬스에 남게. 실패는 예외. */
    fun createSchema()
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

/** 30일 삭제 한 번의 결과 — 지운 건수. 실패했으면 [deleted] 는 null 이고 [failure] 에 예외 종류 이름 */
data class RetentionResult(val ranAt: Instant, val deleted: Int?, val failure: String?)

data class LogCount(val hour: Instant, val service: String, val level: String, val count: Long)

/** 조회 쪽이 쓰는 면 */
interface LogReader {
    fun search(query: LogQuery): List<LogEntry>
    fun stats(from: Instant, to: Instant): List<LogCount>
}

/**
 * PostgreSQL 저장소. 수집·조회·헬스가 서로 다른 연결 풀을 쓴다 — 무거운 조회나 몰려드는 헬스 요청이 연결을 다 잡아도
 * 수집은 밀리지 않는다.
 */
class LogStore(private val ingest: JdbcTemplate, private val query: JdbcTemplate, private val health: JdbcTemplate) : LogSink, LogReader {

    /**
     * 스키마 — 있으면 그대로 둔다. `IF NOT EXISTS` 라도 두 인스턴스가 **동시에 처음** 만들면 PostgreSQL 이 같은 이름을 두 번
     * 만들다 한쪽이 고유 제약 위반으로 실패할 수 있다. 그때는 그 바퀴가 실패로 남고 수집 루프가 다시 시도해 회복한다
     * (두 번째에는 이미 있어 그대로 지나간다).
     */
    override fun createSchema() {
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

    /**
     * 가장 최근에 저장된 로그의 시각(헬스용) — 표가 비었으면 null, DB 에 닿지 못하면 예외. DB 에 닿는가의 확인을 겸한다.
     * 프로세스 메모리가 아니라 DB 에서 읽어 재시작 직후에도, 다른 자리가 저장한 것도 보인다. 기본 키를 거꾸로 1행만 읽는다.
     */
    fun latestStoredAt(): Instant? = health.query(
        "SELECT stream_ms FROM log_entry ORDER BY stream_ms DESC, stream_seq DESC LIMIT 1",
        { row, _ -> Instant.ofEpochMilli(row.getLong("stream_ms")) },
    ).firstOrNull()

    companion object {
        /** 사용자가 넣은 글자를 그대로 찾는다 — `%`·`_` 가 와일드카드로 쓰이지 않게 */
        fun escapeLike(text: String) = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    }
}
