package flink.kafka

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer


object Demo101 extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // Adding KafkaSource
  val kafkaSource = KafkaSource.builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("quickstart-events")
    .setGroupId("0")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()


  val lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")





  // Printing to console what we have consumed
  lines.print()
  env.execute("Read from Kafka")

}
