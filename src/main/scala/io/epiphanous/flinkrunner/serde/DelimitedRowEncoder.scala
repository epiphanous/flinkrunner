package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.ObjectWriter
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

/** Write an element as a csv line
  * @param delimiter
  *   the delimiter, defaults to comma
  * @tparam E
  *   the element type
  */
class DelimitedRowEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends RowEncoder[E]
    with DelimitedCodec {

  @transient
  lazy val writer: ObjectWriter =
    getWriter(delimitedConfig, implicitly[TypeInformation[E]].getTypeClass)

  override def encode(element: E): Try[String] =
    Try(writer.writeValueAsString(element))
}
