package config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object KafkaStreamsConfig {
  val bootstrapServers = "localhost:9092"
  val applicationId = "notification-streams"
  private val defaultStateDir  = "/tmp/kafka-streams"

  def getProperties: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, sys.env.getOrElse("KAFKA_BOOTSTRAP", bootstrapServers))
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,     sys.env.getOrElse("KAFKA_APP_ID",    applicationId))
    props.put(StreamsConfig.STATE_DIR_CONFIG,          sys.env.getOrElse("KAFKA_STATE_DIR", defaultStateDir))

    // Serde
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   "org.apache.kafka.common.serialization.Serdes$StringSerde")
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde")

    // 처리 보장
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE)

    // 소비/반응성
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.POLL_MS_CONFIG, "1000")

    // 단일 브로커 개발 환경
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")

    // 역직렬화 예외 시 계속 진행
    props.put(
      StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
      "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler"
    )

    // (선택) cache 비활성화: 최신 버전에서 deprecated 경고가 뜰 수 있습니다.
    // props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")


    props
  }
}