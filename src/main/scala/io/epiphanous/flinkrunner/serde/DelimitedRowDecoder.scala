package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper

import scala.util.Try

class DelimitedRowDecoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends RowDecoder[E] {

  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper = new CsvMapper()

  @transient
  lazy val reader: ObjectReader =
    mapper.reader(delimitedConfig.intoSchema(mapper.schemaFor(klass)))

  override def decode(line: String): Try[E] =
    Try(reader.readValue(line))
}
