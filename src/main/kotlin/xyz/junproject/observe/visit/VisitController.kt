package xyz.junproject.observe.visit

import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import java.time.Instant

/** 방문 요약 조회 `GET /visits/summary?days=` */
class VisitController(private val reader: VisitReader, private val secret: String) {
    fun summary(request: HttpServletRequest, days: String?): ResponseEntity<out Any> = summary(request, days, Instant.now())

    internal fun summary(request: HttpServletRequest, days: String?, now: Instant): ResponseEntity<out Any> = TODO("visit-tracking V2 구현")
}
