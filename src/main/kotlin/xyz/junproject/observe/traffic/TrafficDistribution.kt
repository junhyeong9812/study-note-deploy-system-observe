package xyz.junproject.observe.traffic

import java.time.Duration
import java.time.Instant

/** 분위수가 든 칸 — [atMostMs] 는 그 칸의 위쪽 경계("≤ 250ms"), 20번째 칸이면 null 이고 [overMs] 가 180000("> 180s") */
data class TrafficQuantile(val atMostMs: Long?, val overMs: Long?)

/** 응답 시간 분포 20칸(규약 4.2) — 칸의 위쪽 경계와 분위수가 든 칸 */
object TrafficDistribution {
    const val BUCKET_COUNT = 20
    val UPPER_BOUNDS_MS: List<Long> = listOf(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000, 20000, 30000, 45000, 60000, 90000, 120000, 180000)

    /**
     * [percent] 분위수가 든 칸(보간하지 않는다). 순위 = ceil(합 × percent / 100)(최소 1), 앞에서부터 센 수가 순위에 닿는 첫 칸.
     * 합이 0 이면 null.
     */
    fun quantile(buckets: List<Long>, percent: Int): TrafficQuantile? {
        val total = buckets.sum()
        if (total == 0L) return null
        val rank = Math.ceilDiv(total * percent, 100L).coerceAtLeast(1)
        var seen = 0L
        val index = buckets.indexOfFirst { seen += it; seen >= rank }
        return if (index < UPPER_BOUNDS_MS.size) TrafficQuantile(UPPER_BOUNDS_MS[index], null) else TrafficQuantile(null, UPPER_BOUNDS_MS.last())
    }
}

/**
 * 서버마다의 수신 상태 — 읽기 진도 · 그것이 마지막으로 나아간 때(관측 서버의 받은 시각) · 순번의 틈 · 마지막 수신 · 처음 본 순번.
 * - 처음 본 순번 = 그 세대에서 받은 **가장 작은** 순번(먼저 커밋된 순번이 아니다 — 더 작은 순번이 늦게 오면 낮아지고 진도는 거기서부터 다시 잇는다).
 * - 읽기 진도: 세대마다 **처음 본 순번**부터 틈 없이 이어진 항목들의 가장 큰 `at`(서버의 값은 세대들 중 가장 큰 것).
 * - 틈난 순번은 그보다 뒤의 순번을 처음 받은 때부터 틈 기다림([TrafficStore] 의 `gapWait`, 기본 10분 — 설정 `observe.traffic.gap-wait`)이 **넘게** 지나도록
 *   오지 않으면 "잃음"으로 확정하고, 진도는 그 틈을 넘어 다음으로 이어진 항목까지 나아간다(확정 판정은 뒤의 항목을 저장할 때 그 받은 시각으로 한다).
 *   정확히 기다림만큼이면 아직 기다린다. 확정 뒤 그 순번이 늦게 오면 저장하고 틈의 수를 하나 줄인다 — 진도는 그대로.
 * - 틈의 수: 세대마다 (가장 큰 순번 − 처음 본 순번 + 1 − 받은 순번의 수)의 합, 세대마다 0 아래로 내려가지 않는다.
 * - [firstSeqSeen]: 가장 최근에 받은 세대에서 처음 본 순번 — 1 이 아니면 관측 서버가 그 세대의 시작을 보지 못했다(그 앞은 틈이 아니다).
 */
data class TrafficServerProgress(
    val server: String,
    val readProgressAt: Instant?,
    val progressAdvancedAt: Instant?,
    val sequenceGaps: Long,
    val lastReceivedAt: Instant?,
    val firstSeqSeen: Long? = null,
)

/** 읽기 진도로 정하는 표시(규약 4.5) */
object TrafficReadProgress {
    val FILLING_MARGIN: Duration = Duration.ofSeconds(30)
    val RECEIVE_DELAY: Duration = Duration.ofMinutes(2)

    /** 칸이 [cellEnd] 에서 끝날 때 "채우는 중"인가 — `읽기 진도 − 30초` 보다 뒤에서 끝나면(같으면 아니다). 읽기 진도가 없으면 늘 채우는 중 */
    fun isFilling(cellEnd: Instant, readProgress: Instant?): Boolean = readProgress == null || cellEnd.isAfter(readProgress.minus(FILLING_MARGIN))

    /** 읽기 진도가 마지막으로 나아간 뒤 2분 **넘게** 지났으면 "수신 지연"(정확히 2분은 아니다). 나아간 적이 없으면 지연 */
    fun isReceiveDelayed(progressAdvancedAt: Instant?, now: Instant): Boolean =
        progressAdvancedAt == null || now.isAfter(progressAdvancedAt.plus(RECEIVE_DELAY))
}
