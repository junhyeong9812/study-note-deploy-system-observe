package xyz.junproject.observe.logalert

import xyz.junproject.observe.threshold.LogRule
import xyz.junproject.observe.threshold.visibleText
import java.time.Duration

/** 감시하는 서비스 한 줄 — 서비스 · 사는 서버 · 기준 */
data class WatchedService(val service: String, val server: String, val threshold: Duration)

/** 설정에서 받아들이지 않은 한 줄 — [entry] 는 다듬은 글자, [reason] 은 까닭의 낱말 */
data class SettingsProblem(val entry: String, val reason: String)

/**
 * 로그 무수신 감시의 설정(⑨ 03) — `LOG_ALERT_SERVICES`(`<서비스>@<사는 서버>=<기준(분)>` 을 쉼표로) · `LOG_ALERT_ALL_THRESHOLD_MINUTES`.
 * [allThreshold] 는 전체 한 줄의 기준(없으면 그 줄은 감시하지 않는다), [problems] 는 받아들이지 않은 줄(줄의 차례대로).
 */
data class LogAlertSettings(val services: List<WatchedService>, val allThreshold: Duration?, val problems: List<SettingsProblem>,
                            /** §16: 받아들였지만 알려 둘 줄(사는 서버가 기대 서버 목록에 없음 등). 뼈대: 아직 채우지 않는다 */
                            val warnings: List<SettingsProblem> = emptyList()) {
    /** 감시하는 것이 하나라도 있는가 — 없으면 판정이 꺼진다 */
    val enabled: Boolean get() = services.isNotEmpty() || allThreshold != null

    /** 기동 로그에 남길 줄 — 감시하는 것 · 받아들이지 않은 줄(다듬은 글자와 까닭) · 아무것도 감시하지 않으면 `disabled` */
    fun startupLines(): List<String> =
        services.map { "log alerts watching ${printable(it.service)}@${printable(it.server)} threshold ${it.threshold.toMinutes()} minutes" } +
            listOfNotNull(allThreshold?.let { "log alerts watching ${LogRule.SILENT_ALL.wireName} threshold ${it.toMinutes()} minutes" }) +
            problems.map { "log alerts not watching ${it.entry}: ${it.reason}" } +
            listOfNotNull(if (enabled) null else "log alerts disabled — nothing to watch")

    companion object {
        /** 기준의 상한(분) — 6시간을 넘는 기준은 감시하지 않는다 */
        const val MAX_THRESHOLD_MINUTES = 360

        /**
         * 설정의 글자를 읽는다 — 줄은 쉼표로(앞뒤 공백 · 빈 줄은 무시). 받아들이지 않은 줄은 그 줄만 빠지고 까닭 하나가 남는다
         * (이름 → 서버 → 기준의 차례로 첫 까닭). 같은 서비스 이름이 두 번 나오면 그 이름의 줄은 모두 빠진다(상태의 열쇠가 서비스라서).
         */
        fun parse(services: String, allThresholdMinutes: String, expectedServers: Set<String> = emptySet()): LogAlertSettings {
            val entries = services.split(',').map { it.trim() }.filter { it.isNotEmpty() }.map { Entry.of(it) }
            val nameCounts = entries.groupingBy { it.name }.eachCount()
            val watched = mutableListOf<WatchedService>()
            val problems = mutableListOf<SettingsProblem>()
            for (entry in entries) {
                val reason = nameProblem(entry.name, nameCounts.getValue(entry.name))
                    ?: serverProblem(entry.server)
                    ?: thresholdProblem(entry.threshold)
                if (reason == null) watched += WatchedService(entry.name, entry.server!!, Duration.ofMinutes(entry.threshold!!.trim().toLong()))
                else problems += SettingsProblem(printable(entry.text), reason)
            }
            val allText = allThresholdMinutes.trim()
            val allReason = if (allText.isEmpty()) null else thresholdProblem(allText)
            if (allReason != null) problems += SettingsProblem(printable("${LogRule.SILENT_ALL.wireName}=$allText"), allReason)
            val allThreshold = if (allText.isEmpty() || allReason != null) null else Duration.ofMinutes(allText.toLong())
            return LogAlertSettings(watched, allThreshold, problems)
        }

        /** 한 줄 — `<이름>@<서버>=<기준>`. `@` 가 없으면 서버가 없고, `=` 가 없으면 기준이 없다 */
        private class Entry(val text: String, val name: String, val server: String?, val threshold: String?) {
            companion object {
                fun of(text: String): Entry {
                    val at = text.indexOf('@')
                    val beforeThreshold = text.substringBefore('=')
                    val threshold = if ('=' in text) text.substringAfter('=') else null
                    return if (at < 0 || at > beforeThreshold.length) Entry(text, beforeThreshold, null, threshold)
                    else Entry(text, beforeThreshold.substring(0, at), beforeThreshold.substring(at + 1), threshold)
                }
            }
        }

        private fun nameProblem(name: String, count: Int): String? = when {
            name == LogSilentJudge.ALL_SERVICES -> "name_reserved"
            name.isEmpty() -> "name_empty"
            hasHiddenCharacters(name) -> "name_hidden_characters"
            count > 1 -> "name_duplicated"
            else -> null
        }

        private fun serverProblem(server: String?): String? = when {
            server.isNullOrEmpty() -> "server_missing"
            server == LogSilentJudge.ALL_SERVICES || hasHiddenCharacters(server) -> "server_invalid"
            else -> null
        }

        private fun thresholdProblem(text: String?): String? {
            val trimmed = text?.trim()
            if (trimmed.isNullOrEmpty()) return "no_threshold"
            val minutes = trimmed.toLongOrNull() ?: return "threshold_not_number"
            return when {
                minutes <= 0 -> "threshold_not_positive"
                minutes > MAX_THRESHOLD_MINUTES -> "threshold_over_6h"
                else -> null
            }
        }

        /** 숨은 문자([visibleText] 가 바꾸는 것)나 짝 없는 서로게이트가 들었는가 */
        private fun hasHiddenCharacters(text: String) = printable(text) != text

        /** 로그에 실을 글자 — 숨은 문자와 짝 없는 서로게이트를 '?' 로 */
        private fun printable(text: String): String = buildString {
            visibleText(text).codePoints().forEach { codePoint ->
                if (Character.getType(codePoint) == Character.SURROGATE.toInt()) append('?') else appendCodePoint(codePoint)
            }
        }
    }
}
