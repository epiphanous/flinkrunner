package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  EmbeddedAvroJsonSerializationSchema,
  JsonSerializationSchema
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.streaming.api.scala.DataStream

/** Kinesis Sink Config
  *
  * Configuration: Configuration:
  *   - stream - required name of the kinesis firehose delivery stream
  *   - aws.region - optional aws region where kinesis is hosted (defaults
  *     to us-east-1)
  *   - aws.endpoint - optional aws kinesis endpoint (defaults to normal
  *     endpoint from configured kinesis region, but you can specify if you
  *     are using localstack)
  *   - client - optional kinesis client properties (aws.region or
  *     aws.endpoint can also be specified here)
  *   - max.batch.size.in.number: the maximum size of a batch of entries
  *     that may be sent to KDS
  *   - max.in.flight.requests: the maximum number of in flight requests
  *     that may exist, if any more in flight requests need to be initiated
  *     once the maximum has been reached, then it will be blocked until
  *     some have completed
  *   - max.buffered.requests: the maximum number of elements held in the
  *     buffer, requests to add elements will be blocked while the number
  *     of elements in the buffer is at the maximum
  *   - max.batch.size.in.bytes: the maximum size of a batch of entries
  *     that may be sent to KDS measured in bytes
  *   - max.time.in.buffer: the maximum amount of time an entry is allowed
  *     to live in the buffer, if any element reaches this age, the entire
  *     buffer will be flushed immediately
  *   - max.record.size.in.bytes: the maximum size of a record the sink
  *     will accept into the buffer, a record of size larger than this will
  *     be rejected when passed to the sink
  *   - fail.on.error: when an exception is encountered while persisting to
  *     Kinesis Data Streams, the job will fail immediately if failOnError
  *     is set
  */
case class KinesisSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Kinesis

  val props: KinesisProperties = KinesisProperties.fromSinkConfig(this)

  def _addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E],
      serializationSchema: SerializationSchema[E]): Unit = {
    val ks = {
      val kb = KinesisStreamsSink
        .builder[E]
        .setKinesisClientProperties(props.clientProperties)
        .setSerializationSchema(serializationSchema)
        .setPartitionKeyGenerator(element => element.$key)
        .setStreamName(props.stream)
        .setFailOnError(props.failOnError)
        .setMaxBatchSize(props.maxBatchSizeInNumber)
        .setMaxBatchSizeInBytes(props.maxBatchSizeInBytes)
        .setMaxInFlightRequests(props.maxInFlightRequests)
        .setMaxBufferedRequests(props.maxBufferedRequests)
        .setMaxTimeInBufferMS(props.maxBufferTime)
      props.maxRecordSizeInBytes
        .map(kb.setMaxRecordSizeInBytes)
        .getOrElse(kb)
    }.build()
    dataStream
      .sinkTo(ks)
      .uid(label)
      .name(label)
      .setParallelism(parallelism)
  }

  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink[E](dataStream, getSerializationSchema[E])

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink[E](dataStream, getAvroSerializationSchema[E, A])

  def getSerializationSchema[E <: ADT: TypeInformation]
      : SerializationSchema[E] =
    new JsonSerializationSchema[E, ADT](this)

  def getAvroSerializationSchema[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation] =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](this)

}
