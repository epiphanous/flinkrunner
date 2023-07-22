package io.epiphanous.flinkrunner.serde

import org.apache.kafka.common.serialization.{
  Deserializer,
  StringDeserializer
}

class SimpleStringDeserializer extends Deserializer[AnyRef] {
  val ds                                                             = new StringDeserializer()
  override def deserialize(topic: String, data: Array[Byte]): AnyRef =
    ds.deserialize(topic, data)
}
