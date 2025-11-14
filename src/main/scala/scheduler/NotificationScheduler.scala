package scheduler

import akka.actor.ActorSystem
import usecase.NotificationUseCase

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{MILLISECONDS, _}

class NotificationScheduler(notificationService: NotificationUseCase,zoneId: ZoneId = ZoneId.systemDefault())(implicit system: ActorSystem, ec: ExecutionContext) {

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  // 매일 새벽 4시에 실행되는 스케줄러
  system.scheduler.scheduleAtFixedRate(
    initialDelay = calculateInitialDelay(zoneId),
    interval = 24.hours
  ) { () =>
    scheduleDailyContentSchedules()
  }

  // 오늘 진행될 컨텐츠 스케줄 ID 조회 후 알림 예약
  private def scheduleDailyContentSchedules(): Unit = {
    println("새벽 4시: 오늘 진행될 컨텐츠 스케줄 ID 조회 중...")

    // 오늘 00:00:00 기준 조회 (atStartOfDay 없이)
    val todayStartStr = LocalDate.now(zoneId).atTime(0, 0, 0).format(formatter)

    try {
      val scheduleIds = notificationService.getTodayContentSchedules(todayStartStr)

      scheduleIds.foreach { scheduleId =>
        try {
          val startAtMillis = notificationService.getScheduleStartTime(scheduleId)
          notificationService.registScheduleNotification(scheduleId, startAtMillis)
        } catch {
          case e: Throwable =>
            println(s"[Scheduler] 등록 실패(scheduleId=$scheduleId): ${e.getMessage}")
        }
      }

      println(s"[Scheduler] 작업 완료: ${scheduleIds.size}건 등록")
    } catch {
      case e: Throwable =>
        println(s"[Scheduler] 스케줄 ID 조회 실패: ${e.getMessage}")
    }
  }

  // 새벽 4시에 실행되도록 초기 지연 시간 계산 (이미 4AM이 지났으면 내일 4AM으로 설정)
  private def calculateInitialDelay(zone: ZoneId): FiniteDuration = {
    val now = ZonedDateTime.now(zone)
    val today4 = now.withHour(4).withMinute(0).withSecond(0).withNano(0)
    val target = if (now.isAfter(today4)) today4.plusDays(1) else today4
    val ms = java.time.Duration.between(now, target).toMillis
    FiniteDuration(math.max(ms, 0L), MILLISECONDS)
  }
}