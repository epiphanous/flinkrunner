package io.epiphanous.flinkrunner.serde
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper

import java.io.OutputStream

/**
 * Encoder for writing an element to a delimited text file output stream
 *
 * @param delimitedConfig
 *   a configuration of delimited file parameters
 * @tparam E
 *   the type to encode into the file
 */
class DelimitedFileEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends Encoder[E] {

  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper = new CsvMapper()

  @transient
  lazy val writer: ObjectWriter =
    mapper.writer(delimitedConfig.intoSchema(mapper.schemaFor(klass)))

  override def encode(element: E, stream: OutputStream): Unit =
    stream.write(writer.writeValueAsBytes(element))
}
