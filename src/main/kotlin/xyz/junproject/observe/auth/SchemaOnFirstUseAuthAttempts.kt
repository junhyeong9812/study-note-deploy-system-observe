package xyz.junproject.observe.auth

import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * 스키마를 처음 DB 에 닿을 때 만든다 — `log_entry` 를 수집 바퀴가 첫 성공 전에 만드는 것과 같은 뜻.
 * 뜰 때 DB 가 없어도 앱은 죽지 않고(연결 풀이 기동 때 닿지 않는다), 그동안의 호출은 [createSchema] 실패를 그대로 예외로 내
 * 경로가 503 이 된다. 다시 닿는 첫 호출이 만들고, 그 뒤로는 묻지 않는다. 만들기에 성공하기 전까지는 호출마다 다시 시도한다.
 * 동시 첫 생성의 한계는 [AuthAttemptStore.createSchema] — 진 쪽 호출 하나가 실패하고 다음 호출에 회복한다.
 */
class SchemaOnFirstUseAuthAttempts(private val delegate: AuthAttempts) : AuthAttempts by delegate {
    private val log = LoggerFactory.getLogger("observe")
    @Volatile private var schemaReady = false
    private val creating = Any()

    private fun ready(): AuthAttempts {
        if (!schemaReady) synchronized(creating) {
            if (!schemaReady) {
                try {
                    delegate.createSchema()
                } catch (error: Exception) {
                    log.error("observe:observe:auth schema failed — will retry on next call: {}: {}", error.javaClass.simpleName, error.message?.take(200))
                    throw error
                }
                schemaReady = true
                log.info("observe:observe:auth schema ready")
            }
        }
        return delegate
    }

    override fun decide(request: AuthAttemptRequest): AuthVerdict = ready().decide(request)
    override fun recordRejected(attempt: AuthRejectedAttempt) = ready().recordRejected(attempt)
    override fun list(query: AuthAttemptQuery): List<AuthAttemptRow> = ready().list(query)
    override fun lockStatus(now: Instant): AuthLockStatus = ready().lockStatus(now)
    override fun deleteOlderThan(cutoff: Instant): Int = ready().deleteOlderThan(cutoff)
}
