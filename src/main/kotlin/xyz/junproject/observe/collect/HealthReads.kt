package xyz.junproject.observe.collect

import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * 헬스 칸(지표 · 통신)이 바깥에 묻는 읽기 — 한꺼번에 가상 스레드에서 시작하고, 합쳐 정한 마감까지만 기다린다.
 * 마감을 넘긴 읽기는 [TimeoutException] 실패로 보고 버려 둔다(가상 스레드라 늦은 읽기가 쌓여도 스레드를 잡지 않는다).
 */
internal object HealthReads {
    private val READERS = Executors.newVirtualThreadPerTaskExecutor()
    private const val UNDEFINED_TABLE = "42P01"   // PostgreSQL: relation does not exist

    fun <T> start(read: () -> T): CompletableFuture<T> = CompletableFuture.supplyAsync({ read() }, READERS)

    /** [deadline] 은 `System.nanoTime()` 기준 — 읽기의 예외는 그대로 */
    fun <T> await(read: CompletableFuture<T>, deadline: Long): Result<T> = try {
        Result.success(read.get(maxOf(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
    } catch (error: ExecutionException) {
        Result.failure(error.cause ?: error)
    } catch (error: TimeoutException) {
        read.cancel(true)
        Result.failure(error)
    } catch (error: InterruptedException) {
        Thread.currentThread().interrupt()
        Result.failure(error)
    }

    /** 표가 아직 없다는 답 — DB 에는 닿은 것이다 */
    fun isTableMissing(error: Throwable) = generateSequence(error) { it.cause }.any { it is SQLException && it.sqlState == UNDEFINED_TABLE }
}
