package io.epiphanous.flinkrunner.serde

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
    extends RowEncoder[E] {

  @transient
  lazy val codec: Codec[E] = Codec(
    implicitly[TypeInformation[E]].getTypeClass,
    delimitedConfig = delimitedConfig
  )

  override def encode(element: E): Try[String] =
    Try(codec.csvWriter.writeValueAsString(element))
}
