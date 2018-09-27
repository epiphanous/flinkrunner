package io.epiphanous.flinkrunner.serialization
import io.circe.{Decoder, Encoder}
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SerializationSchema}

class FlinkEventSerializationDeserializationSchema[E <: FlinkEvent](implicit decoder: Decoder[E], encoder: Encoder[E])
    extends AbstractDeserializationSchema[E]
    with SerializationSchema[E]
    with JsonConfig[E] {

  override def deserialize(message: Array[Byte]): E = deserializeEvent(message)
  override def serialize(event: E): Array[Byte]     = serializeEvent(event)

  def deserializeEvent(message: Array[Byte]): E = deserializeBytes(message)
  def serializeEvent(event: E): Array[Byte]     = serializeToBytes(event)
}
