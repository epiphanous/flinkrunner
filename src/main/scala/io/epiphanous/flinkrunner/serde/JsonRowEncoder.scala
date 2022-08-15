package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

class JsonRowEncoder[E: TypeInformation](
    pretty: Boolean = false,
    sortKeys: Boolean = false)
    extends RowEncoder[E]
    with JsonCodec {

  @transient
  lazy val mapper: JsonMapper = getMapper(pretty, sortKeys)

  @transient
  val writer: ObjectWriter =
    mapper.writerFor(implicitly[TypeInformation[E]].getTypeClass)

  override def encode(element: E): Try[String] = {
    Try(
      writer.writeValueAsString(element) + System
        .lineSeparator()
    )
  }
}
