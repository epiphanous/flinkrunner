package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.avro.AvroCoder
import io.epiphanous.flinkrunner.flink.BaseFlinkJob
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{
  DeserializationSchema,
  Encoder,
  SerializationSchema
}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.connectors.kafka.{
  KafkaDeserializationSchema,
  KafkaSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.serialization.{
  KinesisDeserializationSchema,
  KinesisSerializationSchema
}
import org.apache.flink.streaming.connectors.rabbitmq.{
  RMQDeserializationSchema,
  RMQSinkPublishOptions
}

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getJobInstance[DS, OUT <: ADT](
      name: String,
      runner: FlinkRunner[ADT]): BaseFlinkJob[DS, OUT, ADT]

  def getDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): DeserializationSchema[E] = ???

  def getKafkaDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaDeserializationSchema[E] =
    ???

  def getKafkaRecordSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaRecordSerializationSchema[E] = ???

  def getKafkaRecordDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaRecordDeserializationSchema[E] = ???

  def getKinesisDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KinesisDeserializationSchema[E] = ???

  def getSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): SerializationSchema[E] = ???

  def getKafkaSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaSerializationSchema[E] = ???

  def getKinesisSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KinesisSerializationSchema[E] = ???

  def getEncoder[E <: ADT](name: String, config: FlinkConfig): Encoder[E] =
    ???

  def getAddToJdbcBatchFunction[E <: ADT](
      name: String,
      config: FlinkConfig): AddToJdbcBatchFunction[E] = ???

  def getBucketAssigner[E <: ADT](
      name: String,
      config: FlinkConfig): BucketAssigner[E, String] =
    ???

  def getAvroCoder(name: String, config: FlinkConfig): AvroCoder[_] =
    ???

  def getRMQDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): RMQDeserializationSchema[E] = ???

  def getRabbitPublishOptions[E <: ADT](
      name: String,
      config: FlinkConfig): Option[RMQSinkPublishOptions[E]] = None
}
