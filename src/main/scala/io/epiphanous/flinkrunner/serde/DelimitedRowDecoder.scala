package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

/** Decode a delimited text line into an instance of the requested class.
  *
  * @param delimitedConfig
  *   a delimited codec config (defaults to csv)
  * @tparam E
  *   the type to decode into
  */
class DelimitedRowDecoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig =
      DelimitedConfig.CSV.copy(useHeader = false))
    extends RowDecoder[E] {

  @transient
  lazy val codec: Codec[E] = Codec(
    implicitly[TypeInformation[E]].getTypeClass,
    delimitedConfig = delimitedConfig
  )

  override def decode(line: String): Try[E] =
    Try(codec.csvReader.readValue[E](line))

}
