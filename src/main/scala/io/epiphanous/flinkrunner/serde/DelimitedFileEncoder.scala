package io.epiphanous.flinkrunner.serde
import com.fasterxml.jackson.databind.{
  MapperFeature,
  ObjectMapper,
  ObjectWriter
}
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream

/** Encoder for writing an element to a delimited text file output stream
  *
  * @param delimitedConfig
  *   a configuration of delimited file parameters
  * @tparam E
  *   the type to encode into the file
  */
class DelimitedFileEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends Encoder[E]
    with DelimitedCodec {

  @transient
  lazy val writer: ObjectWriter =
    getWriter(delimitedConfig, implicitly[TypeInformation[E]].getTypeClass)

  override def encode(element: E, stream: OutputStream): Unit =
    stream.write(writer.writeValueAsBytes(element))
}
