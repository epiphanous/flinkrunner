package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.ObjectReader
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

class JsonRowDecoder[E: TypeInformation]
    extends RowDecoder[E]
    with JsonCodec {

  @transient
  lazy val reader: ObjectReader = getReader(
    implicitly[TypeInformation[E]].getTypeClass
  )

  override def decode(line: String): Try[E] =
    Try(reader.readValue(line))
}
