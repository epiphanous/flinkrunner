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
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.connectors.kafka.{
  KafkaDeserializationSchema,
  KafkaSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.serialization.{
  KinesisDeserializationSchema,
  KinesisSerializationSchema
}

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getFlinkConfig(
      args: Array[String],
      sources: Map[String, Seq[Array[Byte]]] = Map.empty,
      optConfig: Option[String] = None) =
    new FlinkConfig[ADT](args, this, sources, optConfig)

  def getJobInstance[DS, OUT <: ADT](
      name: String,
      runner: FlinkRunner[ADT]): BaseFlinkJob[DS, OUT, ADT]

  def getDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): DeserializationSchema[E] = ???

  def getKafkaDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): KafkaDeserializationSchema[E] =
    ???

  def getKinesisDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): KinesisDeserializationSchema[E] = ???

  def getSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): SerializationSchema[E] = ???

  def getKafkaSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): KafkaSerializationSchema[E] = ???

  def getKinesisSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): KinesisSerializationSchema[E] = ???

  def getEncoder[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): Encoder[E] = ???

  def getAddToJdbcBatchFunction[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): AddToJdbcBatchFunction[E] = ???

  def getBucketAssigner[E <: ADT](
      name: String,
      config: FlinkConfig[ADT]): BucketAssigner[E, String] =
    ???

  def getAvroCoder(name: String, config: FlinkConfig[ADT]): AvroCoder[_] =
    ???
}
