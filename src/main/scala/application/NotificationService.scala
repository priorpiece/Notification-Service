package application

import application.out.{FcmPort, KafkaPort, ReservationPort, SchedulePort, UserPort}
import usecase.NotificationUseCase

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class NotificationService(schedulePort: SchedulePort, reservationPort: ReservationPort, userPort: UserPort, fcmPort: FcmPort , kafkaProducerPort : KafkaPort)
  extends NotificationUseCase {

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  //  오늘 진행될 컨텐츠 스케줄 ID 목록 가져오기
  override def getTodayContentSchedules(startTime: String): Seq[String] = {
    schedulePort.getScheduleIdsByStartTime(startTime)
  }

  override def getScheduleStartTime(scheduleId: String): Long = {
    try {
      // ScheduleAdapter(gRPC 클라이언트)를 통해 스케줄 시작 시간 조회
      val startTimeStr = schedulePort.getStartTimeByScheduleId(scheduleId)
      val localDateTime = LocalDateTime.parse(startTimeStr, formatter)

      // 현재 시스템 타임존 적용 후 밀리초 변환
      val millis = localDateTime.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

      println(s"스케줄 ID: $scheduleId - 시작 시간 (밀리초): $millis")
      millis
    } catch {
      case e: Exception =>
        println(s"스케줄 ID: $scheduleId - 시작 시간 조회 실패: ${e.getMessage}")
        System.currentTimeMillis() // 실패 시 현재 시간 반환
    }
  }

  // 컨텐츠 스케줄 ID로 예약된 사용자 목록 조회 후 알림 전송
  override def fetchUsersAndSendNotifications(contentScheduleId: String): Future[Unit] = {
    val userIds = reservationPort.getUsersByContentScheduleId(contentScheduleId)

    // FCM 알림 전송을 Future 리스트로 변환
    val notificationFutures = userIds.map { userId =>
      userPort.getFcmToken(userId) match {
        case Some(fcmToken) =>
          fcmPort.sendPushNotification(fcmToken, "예약 알림", "예약한 컨텐츠 시작 5분 전입니다!")
        case None =>
          println(s"사용자 ${userId}의 FCM 토큰을 찾을 수 없음")
          Future.successful(()) // FCM 토큰이 없는 경우 Future 반환
      }
    }
    // 모든 Future 작업이 완료될 때까지 기다림
    Future.sequence(notificationFutures).map(_ => ())
//    val testFcmToken = "" // 임의의 FCM 토큰 설정
//    val futureResponse: Future[String] = fcmPort.sendPushNotification(
//      token = testFcmToken,
//      title = "테스트 알림",
//      body = "이것은 테스트 메시지입니다."
//    )
//    futureResponse.map { _ =>
//      println("FCM 알림 전송 성공")
//    }.recover {
//      case exception =>
//        println(s"FCM 알림 전송 실패: ${exception.getMessage}")
//    }.map(_ => ())
  }

  override def registScheduleNotification(scheduleId: String, scheduleTime: Long): Unit = {
    val now = System.currentTimeMillis()
    val reminderTime = scheduleTime - TimeUnit.MINUTES.toMillis(5)

    if (reminderTime > now) {
      println(s"Kafka로 5분 전 알림 메시지 전송: $scheduleId")
      kafkaProducerPort.sendNotification(scheduleId, scheduleTime) //  Kafka Producer 호출
    } else {
      println(s"이미 지난 컨텐츠 스케줄은 무시됨: $scheduleId")
    }
  }
}