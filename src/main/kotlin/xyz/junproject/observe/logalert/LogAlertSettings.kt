package xyz.junproject.observe.logalert

import java.time.Duration

/** 감시하는 서비스 한 줄 — 서비스 · 사는 서버 · 기준 */
data class WatchedService(val service: String, val server: String, val threshold: Duration)

/** 설정에서 받아들이지 않은 한 줄 — [entry] 는 다듬은 글자, [reason] 은 까닭의 낱말 */
data class SettingsProblem(val entry: String, val reason: String)

/**
 * 로그 무수신 감시의 설정(⑨ 03). 뼈대(03): 읽는 법은 아직 없다.
 * [allThreshold] 는 전체 한 줄의 기준(없으면 그 줄은 감시하지 않는다), [problems] 는 받아들이지 않은 줄.
 */
data class LogAlertSettings(val services: List<WatchedService>, val allThreshold: Duration?, val problems: List<SettingsProblem>) {
    /** 감시하는 것이 하나라도 있는가 — 없으면 판정이 꺼진다 */
    val enabled: Boolean get() = services.isNotEmpty() || allThreshold != null

    /** 기동 로그에 남길 줄 — 뼈대: 아직 없다 */
    fun startupLines(): List<String> = TODO("03: 기동 로그의 줄")

    companion object {
        /** 기준의 상한(분) — 6시간을 넘는 기준은 감시하지 않는다 */
        const val MAX_THRESHOLD_MINUTES = 360

        /** 설정의 글자를 읽는다 — 뼈대: 아직 없다 */
        fun parse(services: String, allThresholdMinutes: String): LogAlertSettings = TODO("03: 설정 읽기")
    }
}
