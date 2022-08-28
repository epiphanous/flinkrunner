package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.ObjectReader
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

class DelimitedRowDecoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig =
      DelimitedConfig.CSV.copy(useHeader = false))
    extends RowDecoder[E]
    with DelimitedCodec {

  @transient
  lazy val reader: ObjectReader =
    getReader(delimitedConfig, implicitly[TypeInformation[E]].getTypeClass)

  override def decode(line: String): Try[E] =
    Try(reader.readValue[E](line))
}
