package xyz.junproject.observe.visit

import xyz.junproject.observe.log.RetentionResult
import java.time.Instant

/** 방문 수집 스레드와 하루 한 번 90일 삭제 */
class VisitSchedule(private val collector: VisitCollector, private val retention: VisitRetention) {
    val lastRetention: RetentionResult? get() = TODO("visit-tracking V2 구현")

    fun start(): Unit = TODO("visit-tracking V2 구현")
    fun stop(): Unit = TODO("visit-tracking V2 구현")
    fun deleteOldVisits(): Unit = TODO("visit-tracking V2 구현")
    internal fun deleteOldVisits(now: Instant): Int? = TODO("visit-tracking V2 구현")
}
