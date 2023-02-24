package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  EmbeddedAvroJsonSerializationSchema,
  JsonSerializationSchema
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

/** AWS kinesis firehose sink config.
  *
  * Follow the instructions from the <a
  * href="https://docs.aws.amazon.com/firehose/latest/dev/basic-create.html">Amazon
  * Kinesis Data Firehose Developer Guide</a> to setup a Kinesis Data
  * Firehose delivery stream.
  *
  * Configuration:
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
  *
  * @param name
  *   name of the sink
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class FirehoseSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with LazyLogging {
  override def connector: FlinkConnectorName =
    FlinkConnectorName.Firehose

  val props: KinesisProperties = KinesisProperties.fromSinkConfig(this)

  override def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink(dataStream, getSerializationSchema[E])

  override def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink(
      dataStream,
      getAvroSerializationSchema[E, A]
    )

  def _getSink[E <: ADT](
      dataStream: DataStream[E],
      serializationSchema: SerializationSchema[E]): DataStreamSink[E] = {
    val kfs = {
      val k = KinesisFirehoseSink
        .builder[E]()
        .setFirehoseClientProperties(props.clientProperties)
        .setSerializationSchema(serializationSchema)
        .setDeliveryStreamName(props.stream)
        .setFailOnError(props.failOnError)
        .setMaxInFlightRequests(props.maxInFlightRequests)
        .setMaxBufferedRequests(props.maxBufferedRequests)
        .setMaxBatchSize(props.maxBatchSizeInNumber)
        .setMaxBatchSizeInBytes(props.maxBatchSizeInBytes)
        .setMaxTimeInBufferMS(props.maxBufferTime)
      props.maxRecordSizeInBytes
        .map(k.setMaxBatchSizeInBytes)
        .getOrElse(k)
    }.build()
    dataStream.sinkTo(kfs).setParallelism(parallelism)
  }

  def getSerializationSchema[E <: ADT: TypeInformation]
      : SerializationSchema[E] =
    new JsonSerializationSchema[E, ADT](this)

  def getAvroSerializationSchema[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation] =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](this)
}
