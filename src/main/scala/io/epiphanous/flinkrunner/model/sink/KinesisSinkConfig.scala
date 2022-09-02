package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kinesis
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent}
import io.epiphanous.flinkrunner.serde.JsonSerializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

/**   - maxBatchSize: the maximum size of a batch of entries that may be
  *     sent to KDS
  *   - maxInFlightRequests: the maximum number of in flight requests that
  *     may exist, if any more in flight requests need to be initiated once
  *     the maximum has been reached, then it will be blocked until some
  *     have completed
  *   - maxBufferedRequests: the maximum number of elements held in the
  *     buffer, requests to add elements will be blocked while the number
  *     of elements in the buffer is at the maximum
  *   - maxBatchSizeInBytes: the maximum size of a batch of entries that
  *     may be sent to KDS measured in bytes
  *   - maxTimeInBufferMS: the maximum amount of time an entry is allowed
  *     to live in the buffer, if any element reaches this age, the entire
  *     buffer will be flushed immediately
  *   - maxRecordSizeInBytes: the maximum size of a record the sink will
  *     accept into the buffer, a record of size larger than this will be
  *     rejected when passed to the sink
  *   - failOnError: when an exception is encountered while persisting to
  *     Kinesis Data Streams, the job will fail immediately if failOnError
  *     is set
  */
case class KinesisSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = Kinesis
) extends SinkConfig[ADT]
    with LazyLogging {

  val stream: String = config.getString(pfx("stream"))

  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] = {
    dataStream
      .sinkTo(
        KinesisStreamsSink
          .builder[E]
          .setStreamName(stream)
          .setFailOnError(true)
          .setSerializationSchema(getSerializationSchema[E])
          .setKinesisClientProperties(properties)
          .build()
      )
      .uid(label)
      .name(label)
  }

  def getSerializationSchema[E <: ADT: TypeInformation]
      : SerializationSchema[E] =
    new JsonSerializationSchema[E, ADT](this)
}
