package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

class JsonRowEncoder[E: TypeInformation](
    jsonConfig: JsonConfig = JsonConfig())
    extends RowEncoder[E] {

  @transient
  lazy val codec: Codec[E] =
    Codec(implicitly[TypeInformation[E]].getTypeClass, jsonConfig)

  override def encode(element: E): Try[String] = {
    Try(
      codec.jsonWriter.writeValueAsString(element) + jsonConfig.endOfLine
    )
  }
}
