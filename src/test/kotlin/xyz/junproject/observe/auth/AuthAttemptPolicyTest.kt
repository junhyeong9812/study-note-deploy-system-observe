package xyz.junproject.observe.auth

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 명세 ② 잠금: 계정 기준 연속 5회 실패 → 15분 잠금. 잠금 중에는 맞는 비밀번호도 거절. 성공하면 0.
 * 세부: 잠금이 풀린 뒤에는 0 부터. 잠금 중의 시도는 횟수를 늘리지도 잠금을 연장하지도 않는다.
 */
class AuthAttemptPolicyTest {
    private val policy = AuthAttemptPolicy()
    private val at = Instant.parse("2026-11-22T03:00:00Z")

    @Test
    fun `기본 수치는 명세 그대로 5회·15분·90일이다`() {
        assertEquals(5, policy.failureLimit)
        assertEquals(Duration.ofMinutes(15), policy.lockDuration)
        assertEquals(Duration.ofDays(90), policy.retention)
    }

    @Test
    fun `첫 실패는 거절·failure·횟수 1 이고 잠그지 않는다`() {
        val judgement = policy.judge(AuthLockState(), credentialOk = false, at = at)
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.FAILURE, lockedUntil = null, lockStarted = false, failures = 1), judgement.verdict)
        assertEquals(AuthLockState(1, null), judgement.nextState)
    }

    @Test
    fun `네 번째 실패까지는 잠그지 않는다`() {
        val judgement = policy.judge(AuthLockState(3, null), credentialOk = false, at = at)
        assertFalse(judgement.verdict.lockStarted)
        assertNull(judgement.verdict.lockedUntil)
        assertEquals(4, judgement.verdict.failures)
        assertEquals(AuthLockState(4, null), judgement.nextState)
    }

    @Test
    fun `다섯 번째 실패에서 잠금이 시작되고 끝은 그 시각 + 15분이다`() {
        val judgement = policy.judge(AuthLockState(4, null), credentialOk = false, at = at)
        val until = at.plus(Duration.ofMinutes(15))
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.FAILURE, lockedUntil = until, lockStarted = true, failures = 5), judgement.verdict)
        assertEquals(AuthLockState(5, until), judgement.nextState)
    }

    @Test
    fun `잠금 중에는 맞는 비밀번호도 locked 로 거절하고 횟수·잠금 끝이 그대로다`() {
        val until = at.plus(Duration.ofMinutes(10))
        val state = AuthLockState(5, until)
        val judgement = policy.judge(state, credentialOk = true, at = at)
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.LOCKED, lockedUntil = until, lockStarted = false, failures = 5), judgement.verdict)
        assertEquals(state, judgement.nextState, "잠금 중의 시도가 상태를 바꿨다")
    }

    @Test
    fun `잠금 중의 실패도 locked 이고 횟수를 늘리거나 잠금을 연장하지 않는다`() {
        val until = at.plus(Duration.ofMinutes(10))
        val state = AuthLockState(5, until)
        val judgement = policy.judge(state, credentialOk = false, at = at)
        assertEquals(AuthOutcome.LOCKED, judgement.verdict.outcome)
        assertFalse(judgement.verdict.lockStarted, "이미 걸린 잠금에서 lock_started 가 다시 켜지면 알림이 겹친다")
        assertEquals(until, judgement.verdict.lockedUntil)
        assertEquals(5, judgement.verdict.failures)
        assertEquals(state, judgement.nextState)
    }

    @Test
    fun `잠금 끝 1ms 전까지는 잠겨 있다`() {
        val until = at.plus(Duration.ofMinutes(15))
        val judgement = policy.judge(AuthLockState(5, until), credentialOk = true, at = until.minusMillis(1))
        assertEquals(AuthOutcome.LOCKED, judgement.verdict.outcome)
        assertFalse(judgement.verdict.allowed)
    }

    @Test
    fun `잠금 끝 시각이 되면 풀리고 0 부터 센다`() {
        val until = at.plus(Duration.ofMinutes(15))
        val failed = policy.judge(AuthLockState(5, until), credentialOk = false, at = until)
        assertEquals(AuthVerdict(allowed = false, outcome = AuthOutcome.FAILURE, lockedUntil = null, lockStarted = false, failures = 1), failed.verdict)
        assertEquals(AuthLockState(1, null), failed.nextState)

        val succeeded = policy.judge(AuthLockState(5, until), credentialOk = true, at = until.plusSeconds(60))
        assertEquals(AuthVerdict(allowed = true, outcome = AuthOutcome.SUCCESS, lockedUntil = null, lockStarted = false, failures = 0), succeeded.verdict)
        assertEquals(AuthLockState(0, null), succeeded.nextState)
    }

    @Test
    fun `성공하면 허용이고 실패 횟수는 0 이 된다`() {
        val judgement = policy.judge(AuthLockState(4, null), credentialOk = true, at = at)
        assertEquals(AuthVerdict(allowed = true, outcome = AuthOutcome.SUCCESS, lockedUntil = null, lockStarted = false, failures = 0), judgement.verdict)
        assertEquals(AuthLockState(0, null), judgement.nextState)
    }

    @Test
    fun `풀린 뒤 다시 연속 5회를 채워야 다시 잠긴다`() {
        val until = at.plus(Duration.ofMinutes(15))
        var state = AuthLockState(5, until)
        val verdicts = (0 until 5).map { index ->
            policy.judge(state, credentialOk = false, at = until.plusSeconds(index.toLong())).also { state = it.nextState }.verdict
        }
        assertEquals(listOf(1, 2, 3, 4, 5), verdicts.map { it.failures })
        assertEquals(listOf(false, false, false, false, true), verdicts.map { it.lockStarted })
        assertEquals(until.plusSeconds(4).plus(Duration.ofMinutes(15)), verdicts.last().lockedUntil)
    }

    @Test
    fun `설정으로 준 수치를 쓴다`() {
        val strict = AuthAttemptPolicy(failureLimit = 3, lockDuration = Duration.ofMinutes(1))
        val judgement = strict.judge(AuthLockState(2, null), credentialOk = false, at = at)
        assertTrue(judgement.verdict.lockStarted)
        assertEquals(at.plus(Duration.ofMinutes(1)), judgement.verdict.lockedUntil)
    }

    @Test
    fun `이어 보기 기준은 글자로 나갔다가 같은 값으로 되읽힌다`() {
        val cursor = AuthAttemptCursor(Instant.parse("2026-11-22T03:00:00.123Z"), UUID.fromString("0b9c1d0e-8a3f-4c55-9a51-2f7e1c3d4b6a"))
        assertEquals(cursor, AuthAttemptCursor.parse(cursor.toString()))
        for (text in listOf("", "abc", "2026-11-22T03:00:00Z", "0b9c1d0e-8a3f-4c55-9a51-2f7e1c3d4b6a")) assertNull(AuthAttemptCursor.parse(text), text)
    }

    @Test
    fun `결과 낱말은 명세의 글자 그대로다`() {
        assertEquals(listOf("success", "failure", "locked", "rate_limited"), AuthOutcome.entries.map { it.wireName })
    }
}
