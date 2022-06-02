package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{
  MapperFeature,
  ObjectMapper,
  ObjectWriter,
  SerializationFeature
}

import scala.util.Try

class JsonRowEncoder[E: TypeInformation](
    pretty: Boolean = false,
    sortKeys: Boolean = false)
    extends RowEncoder[E] {

  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper: ObjectMapper = new ObjectMapper()
    .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, sortKeys)
    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, sortKeys)
    .configure(SerializationFeature.INDENT_OUTPUT, pretty)

  @transient
  val writer: ObjectWriter = mapper.writerFor(klass)

  override def encode(element: E): Try[String] = {
    Try(
      writer.writeValueAsString(element) + System
        .lineSeparator()
    )
  }
}
