package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kinesis
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.JsonSerializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties

/**
 *   - maxBatchSize: the maximum size of a batch of entries that may be
 *     sent to KDS
 *   - maxInFlightRequests: the maximum number of in flight requests that
 *     may exist, if any more in flight requests need to be initiated once
 *     the maximum has been reached, then it will be blocked until some
 *     have completed
 *   - maxBufferedRequests: the maximum number of elements held in the
 *     buffer, requests to add elements will be blocked while the number of
 *     elements in the buffer is at the maximum
 *   - maxBatchSizeInBytes: the maximum size of a batch of entries that may
 *     be sent to KDS measured in bytes
 *   - maxTimeInBufferMS: the maximum amount of time an entry is allowed to
 *     live in the buffer, if any element reaches this age, the entire
 *     buffer will be flushed immediately
 *   - maxRecordSizeInBytes: the maximum size of a record the sink will
 *     accept into the buffer, a record of size larger than this will be
 *     rejected when passed to the sink
 *   - failOnError: when an exception is encountered while persisting to
 *     Kinesis Data Streams, the job will fail immediately if failOnError
 *     is set
 *
 * @param config
 *   @param connector
 * @param name
 *   @param stream
 * @param properties
 */
case class KinesisSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    properties: Properties)
    extends SinkConfig
    with LazyLogging {
  def getSerializationSchema[E <: FlinkEvent: TypeInformation]
      : SerializationSchema[E] =
    new JsonSerializationSchema[E](this)
}
