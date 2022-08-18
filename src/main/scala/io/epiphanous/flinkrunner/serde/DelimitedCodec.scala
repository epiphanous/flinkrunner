package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.{
  MapperFeature,
  ObjectReader,
  ObjectWriter,
  SerializationFeature
}
import com.fasterxml.jackson.dataformat.csv.{CsvGenerator, CsvMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

trait DelimitedCodec {

  def getMapper: CsvMapper = CsvMapper
    .builder()
    .addModule(DefaultScalaModule)
    .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, false)
    .configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, false)
    .build()

  def getWriter[E](
      delimitedConfig: DelimitedConfig,
      typeClass: Class[E]): ObjectWriter = {
    val mapper = getMapper
    mapper.writer(delimitedConfig.intoSchema(mapper.schemaFor(typeClass)))
  }

  def getReader[E](
      delimitedConfig: DelimitedConfig,
      typeClass: Class[E]): ObjectReader = {
    val mapper = getMapper
    mapper.reader(delimitedConfig.intoSchema(mapper.schemaFor(typeClass)))
  }

}
