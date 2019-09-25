package io.epiphanous.flinkrunner
import java.util.Properties

import io.epiphanous.flinkrunner.flink.{BaseFlinkJob, FlinkJob}
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getJobInstance(name: String): BaseFlinkJob[_, _ <: ADT]

  def getDeserializationSchema: DeserializationSchema[ADT] = ???

  def getKafkaDeserializationSchema: KafkaDeserializationSchema[ADT] = ???

  def getSerializationSchema: SerializationSchema[ADT] = ???

  def getKeyedSerializationSchema: KeyedSerializationSchema[ADT] = ???

  def getEncoder: Encoder[ADT] = ???

  def getAddToJdbcBatchFunction: AddToJdbcBatchFunction[ADT] = ???

  def getBucketAssigner(props: Properties): BucketAssigner[ADT, String] = ???
}
