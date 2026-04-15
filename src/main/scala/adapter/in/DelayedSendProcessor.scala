package adapter.in

import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import usecase.NotificationUseCase

import java.time.Duration
import io.circe.parser.decode
import kafka.{NotificationMessage, TimeKey}
import kafka.NotificationMessage._

class DelayedSendProcessor(
                            notificationService: NotificationUseCase,
                            storeName: String,
                            tickMillis: Long = 1000L
                          ) extends Processor[String, String, String, String] {

  private var ctx: ProcessorContext[String, String] = _
  private var store: KeyValueStore[String, Array[Byte]] = _

  override def init(context: ProcessorContext[String, String]): Unit = {
    ctx = context
    store = ctx.getStateStore(storeName).asInstanceOf[KeyValueStore[String, Array[Byte]]]
    ctx.schedule(Duration.ofMillis(tickMillis), PunctuationType.WALL_CLOCK_TIME, _ => onTick())
  }

  override def process(record: Record[String, String]): Unit = {
    decode[NotificationMessage](record.value()) match {
      case Right(m) =>
        val k = TimeKey.build(m.reminderTime, m.scheduleId)
        store.put(k, record.value().getBytes("UTF-8"))
      case Left(err) =>
        println(s"[Processor] 수신 파싱 실패: $err, value=${record.value()}")
    }
  }

  private def onTick(): Unit = {
    val now = System.currentTimeMillis()
    val it: KeyValueIterator[String, Array[Byte]] =
      store.range(TimeKey.lowerBound, TimeKey.upperBound(now))

    try {
      while (it.hasNext) {
        val kv   = it.next()
        val json = new String(kv.value, "UTF-8")
        decode[NotificationMessage](json) match {
          case Right(m) =>
            /*이상한 스케쥴 값 예외*/
            if (!m.scheduleId.forall(_.isDigit)) {
              println(s"[Skip] invalid scheduleId=${m.scheduleId}, delete only")
              store.delete(kv.key)
            } else {
              notificationService.fetchUsersAndSendNotifications(m.scheduleId)
              store.delete(kv.key)
            }
          case Left(err) =>
            println(s"[Processor] JSON 파싱 실패: $err, value=$json")
            store.delete(kv.key)
        }
      }
    } finally it.close()
  }
}