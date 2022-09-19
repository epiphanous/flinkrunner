package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

class JsonRowDecoder[E: TypeInformation] extends RowDecoder[E] {

  @transient
  lazy val codec: Codec[E] = Codec(
    implicitly[TypeInformation[E]].getTypeClass
  )

  override def tryDecode(line: String): Try[E] =
    Try(codec.jsonReader.readValue[E](line))
}
