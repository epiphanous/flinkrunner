package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.flink.BaseFlinkJob
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{
  DeserializationSchema,
  Encoder,
  SerializationSchema
}
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.connectors.kafka.{
  KafkaDeserializationSchema,
  KafkaSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.serialization.{
  KinesisDeserializationSchema,
  KinesisSerializationSchema
}

import java.util.Properties

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getJobInstance(
      name: String,
      config: FlinkConfig): BaseFlinkJob[_, _ <: ADT]

  def getDeserializationSchema(
      name: String,
      config: FlinkConfig): DeserializationSchema[ADT] = ???

  def getKafkaDeserializationSchema(
      name: String,
      config: FlinkConfig): KafkaDeserializationSchema[ADT] =
    ???

  def getKinesisDeserializationSchema(
      name: String,
      config: FlinkConfig): KinesisDeserializationSchema[ADT] = ???

  def getSerializationSchema(
      name: String,
      config: FlinkConfig): SerializationSchema[ADT] = ???

  def getKafkaSerializationSchema(
      name: String,
      config: FlinkConfig): KafkaSerializationSchema[ADT] = ???

  def getKinesisSerializationSchema(
      name: String,
      config: FlinkConfig): KinesisSerializationSchema[ADT] = ???

  def getEncoder(name: String, config: FlinkConfig): Encoder[ADT] = ???

  def getAddToJdbcBatchFunction(
      name: String,
      config: FlinkConfig): AddToJdbcBatchFunction[ADT] = ???

  def getBucketAssigner(
      name: String,
      config: FlinkConfig): BucketAssigner[ADT, String] =
    ???
}
