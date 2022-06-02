package io.epiphanous.flinkrunner.serde
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper

import scala.util.Try

/**
 * Write an element as a csv line
 * @param delimiter
 *   the delimiter, defaults to comma
 * @tparam E
 *   the element type
 */
class DelimitedRowEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends RowEncoder[E] {
  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper = new CsvMapper()

  @transient
  lazy val writer: ObjectWriter =
    mapper.writer(delimitedConfig.intoSchema(mapper.schemaFor(klass)))

  override def encode(element: E): Try[String] =
    Try(writer.writeValueAsString(element) + System.lineSeparator())
}
