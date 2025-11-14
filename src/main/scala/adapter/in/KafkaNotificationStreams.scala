package adapter.in

import config.KafkaStreamsConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
import org.apache.kafka.streams.kstream.{Consumed, KStream, Named, Predicate, Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}
import usecase.NotificationUseCase

import java.util.Properties

class KafkaNotificationStreams(notificationService: NotificationUseCase) {
  private val topic = "scheduled_notifications"
  private val store = "delayed-send-store"

  def startStream(): Unit = {
    val props: Properties = KafkaStreamsConfig.getProperties
    val builder = new StreamsBuilder()

    val storeBuilder =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(store),
        Serdes.String(),
        Serdes.ByteArray()
      )
    builder.addStateStore(storeBuilder)

    /*multi thread를 위함*/
    val supplier = new ProcessorSupplier[String, String, String, String] {
      override def get(): DelayedSendProcessor =
        new DelayedSendProcessor(notificationService, store, tickMillis = 1000L)
    }

    // Kafka에서 예약된 알림 메시지를 가져옴
    builder
      .stream[String, String](topic, Consumed.`with`(Serdes.String(), Serdes.String()))
      .process(supplier, Named.as("delayed-send-processor"), store)

    val topology = builder.build()
    println(s"[Streams] Topology:\n${topology.describe()}")

    val streams = new KafkaStreams(topology, props)

    // 상태 변화 감시
    streams.setStateListener { (newState, oldState) =>
      println(s"[Streams] State changed: $oldState → $newState")
    }

    // 예외 핸들러
    streams.setUncaughtExceptionHandler { ex: Throwable =>
      ex.printStackTrace()
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
    }

    // Streams 시작
    streams.start()
    sys.addShutdownHook {
      println("[Streams] Shutdown hook triggered, closing streams...")
      streams.close()
    }
  }
}