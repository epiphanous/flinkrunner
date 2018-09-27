package io.epiphanous.flinkrunner.util

import java.nio.charset.StandardCharsets

import io.circe.Encoder
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.serialization.JsonConfig
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class KeyedFlinkEventSerializationSchema[E <: FlinkEvent](implicit encoder: Encoder[E])
    extends KeyedSerializationSchema[E]
    with JsonConfig[E] {

  override def serializeKey(event: E): Array[Byte] = event.$key.getBytes(StandardCharsets.UTF_8)

  override def serializeValue(event: E): Array[Byte] = serializeValueWithEncoder(event)

  def serializeValueWithEncoder(event: E): Array[Byte] =
    serializeToBytes(event)

  override def getTargetTopic(event: E): String = null
}
