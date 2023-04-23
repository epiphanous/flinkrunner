package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.sink.KinesisSinkConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema

import java.nio.ByteBuffer

class JsonKinesisSerializationSchema[
    E <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    kinesisSinkConfig: KinesisSinkConfig[ADT])
    extends KinesisSerializationSchema[E] {
  val jsonSerializationSchema =
    new JsonSerializationSchema[E, ADT](kinesisSinkConfig)

  override def serialize(element: E): ByteBuffer =
    ByteBuffer.wrap(jsonSerializationSchema.serialize(element))

  override def getTargetStream(element: E): String =
    kinesisSinkConfig.props.stream
}
