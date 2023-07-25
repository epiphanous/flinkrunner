package io.epiphanous.flinkrunner.serde

import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

class SimpleStringSerializer extends Serializer[AnyRef] {
  val ss = new StringSerializer()

  override def serialize(topic: String, data: AnyRef): Array[Byte] =
    data match {
      case str: String => ss.serialize(topic, str)
      case other       => ss.serialize(topic, other.toString)
    }

}
