package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.flink.BaseFlinkJob
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}

import java.util.Properties

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getJobInstance(name: String): BaseFlinkJob[_, _ <: ADT]

  def getDeserializationSchema(sourceConfig: SourceConfig): DeserializationSchema[ADT] = ???

  def getKafkaDeserializationSchema(sourceConfig: KafkaSourceConfig): KafkaDeserializationSchema[ADT] = ???

  def getSerializationSchema(sinkConfig: SinkConfig): SerializationSchema[ADT] = ???

  def getKafkaSerializationSchema(sinkConfig: KafkaSinkConfig): KafkaSerializationSchema[ADT] = ???

  def getEncoder(sinkConfig: SinkConfig): Encoder[ADT] = ???

  def getAddToJdbcBatchFunction(sinkConfig: SinkConfig): AddToJdbcBatchFunction[ADT] = ???

  def getBucketAssigner(props: Properties): BucketAssigner[ADT, String] = ???
}
