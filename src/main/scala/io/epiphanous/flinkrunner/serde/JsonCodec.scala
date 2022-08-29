package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

trait JsonCodec {

  def getMapper(
      pretty: Boolean = false,
      sortKeys: Boolean = false): JsonMapper =
    JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .addModule(new JavaTimeModule)
      .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, sortKeys)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, sortKeys)
      .configure(SerializationFeature.INDENT_OUTPUT, pretty)
      .configure(
        DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE,
        false
      )
      .build()

  def getWriter[E](
      pretty: Boolean = false,
      sortKeys: Boolean = false,
      typeClass: Class[E]): ObjectWriter =
    getMapper(pretty, sortKeys).writerFor(typeClass)

  def getReader[E](typeClass: Class[E]): ObjectReader =
    getMapper().readerFor(typeClass)

}
