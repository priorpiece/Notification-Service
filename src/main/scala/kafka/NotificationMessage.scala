package kafka

import io.circe.Decoder
import io.circe.generic.decoding.DerivedDecoder.deriveDecoder

/** 카프카에 적재/소비하는 예약 알림 메시지 */
final case class NotificationMessage(
                                      scheduleId: String,
                                      reminderTime: Long
                                    )
object NotificationMessage {
  implicit val decoder: Decoder[NotificationMessage] = deriveDecoder
}