package io.epiphanous.flinkrunner

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

/**
 * A trait for creating a factory of jobs and related serialization and
 * deserialization schemas for those jobs. To successfully use this
 * library, implement the required methods of this trait.
 * @tparam ADT
 *   an algebraic data type fall all input and output job types
 */
trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  /**
   * Provide an instance of a named flink job
   * @param name
   *   name of the job
   * @param runner
   *   a [[FlinkRunner]] [ADT]
   * @tparam DS
   *   a flink data stream input type
   * @tparam OUT
   *   an ADT type that is the output type of the job
   * @return
   *   Subclass of [[BaseFlinkJob]] [DS, OUT, ADT]
   */
  def getJobInstance[DS, OUT <: ADT](
      name: String,
      runner: FlinkRunner[ADT]): BaseFlinkJob[DS, OUT, ADT]

  /**
   * Provide a deserialization schema for a kafka or kinesis topic
   * @param name
   *   name of the source
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[DeserializationSchema]] [E]
   */
  def getDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): DeserializationSchema[E] = ???

  /**
   * Provide a deserialization schema for a kafka source
   * @param name
   *   name of the source
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KafkaDeserializationSchema]] [E]
   */
  def getKafkaDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaDeserializationSchema[E] =
    ???

  /**
   * Provide a record serialization schema for a kafka sink
   * @param name
   *   name of the sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KafkaRecordSerializationSchema]] [E]
   */
  def getKafkaRecordSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaRecordSerializationSchema[E] = ???

  /**
   * Provide a record deserialization schema for a kafka source
   * @param name
   *   the name of the source
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KafkaRecordDeserializationSchema]] [E]
   */
  def getKafkaRecordDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaRecordDeserializationSchema[E] = ???

  /**
   * Provide a deserialization schema for a kinesis source
   * @param name
   *   name of the kinesis source
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KinesisDeserializationSchema]] [E]
   */
  def getKinesisDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KinesisDeserializationSchema[E] = ???

  /**
   * Provide a serialization schema for writing to a kafka or kinesis sink
   * @param name
   *   name of the sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   ad ADT type
   * @return
   *   [[SerializationSchema]] [E]
   */
  def getSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): SerializationSchema[E] = ???

  /**
   * Provide a kafka serialization schema for writing to a kafka sink
   * @param name
   *   the name of the kafka sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KafkaSerializationSchema]] [E]
   */
  def getKafkaSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KafkaSerializationSchema[E] = ???

  /**
   * Provide a kinesis serialization schema for writing to a kinesis sink
   * @param name
   *   the name of the kinesis sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[KinesisSerializationSchema]] [E]
   */
  def getKinesisSerializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): KinesisSerializationSchema[E] = ???

  /**
   * Provide an encoder to write to a streaming file sink
   * @param name
   *   the name of the streaming file sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[Encoder]] [E]
   */
  def getEncoder[E <: ADT](name: String, config: FlinkConfig): Encoder[E] =
    ???

  /**
   * Provide a function to add an event to a jdbc sink
   * @param name
   *   name of the jdbc sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[AddToJdbcBatchFunction]] [E]
   */
  def getAddToJdbcBatchFunction[E <: ADT](
      name: String,
      config: FlinkConfig): AddToJdbcBatchFunction[E] = ???

  /**
   * Provide a flink bucket assigner for writing to a streaming file sink
   * @param name
   *   the name of the streaming file sink
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[BucketAssigner]] [E, String]
   */
  def getBucketAssigner[E <: ADT](
      name: String,
      config: FlinkConfig): BucketAssigner[E, String] =
    ???

  /**
   * Provide a deserialization schema for reading from a rabbit mq source
   * @param name
   *   the source name
   * @param config
   *   a [[FlinkConfig]]
   * @tparam E
   *   an ADT type
   * @return
   *   [[RMQDeserializationSchema]] [E]
   */
  def getRMQDeserializationSchema[E <: ADT](
      name: String,
      config: FlinkConfig): RMQDeserializationSchema[E] = ???

  /**
   * Provide an optional rabbit mq sink publish options object
   * @param name
   *   Name of the rabbit sink
   * @param config
   *   [[FlinkConfig]]
   * @tparam E
   *   @return [[Option]] `[` [[RMQSinkPublishOptions]] [E] `]`
   */
  def getRabbitPublishOptions[E <: ADT](
      name: String,
      config: FlinkConfig): Option[RMQSinkPublishOptions[E]] = None
}
