package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.{MapperFeature, SerializationFeature}
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

trait JsonCodec {

  def getMapper(
      pretty: Boolean = false,
      sortKeys: Boolean = false): JsonMapper =
    JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, sortKeys)
      .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, sortKeys)
      .configure(SerializationFeature.INDENT_OUTPUT, pretty)
      .build()

}
