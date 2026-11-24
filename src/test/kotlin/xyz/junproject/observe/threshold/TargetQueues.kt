package xyz.junproject.observe.threshold

import java.time.Instant

/** 시험용 — 알림의 대상(§16: 서버 · 규칙의 묶음 · 대상 이름, 디스크 경고 · 위험은 한 묶음 `"disk"`). 구현의 [AlertTarget.of] 와 같은 규칙을 시험 쪽에서 따로 적는다 */
fun targetOf(alert: ResourceAlert) = AlertTarget(
    alert.server,
    if (alert.rule == ResourceRule.DISK_WARNING || alert.rule == ResourceRule.DISK_CRITICAL) "disk" else alert.rule.wireName,
    alert.targetName,
)

/** 가짜 보낼 표의 [AlertQueue.pendingByTarget] — 대상마다 오래된 것부터 [perTarget] 건과 마지막 시도 시각 */
fun targetQueues(queued: List<QueuedAlert>, lastAttempt: Map<AlertTarget, Instant>, perTarget: Int): List<TargetQueue> =
    queued.groupBy { targetOf(it.alert) }.map { (target, alerts) -> TargetQueue(target, lastAttempt[target], alerts.take(perTarget)) }
