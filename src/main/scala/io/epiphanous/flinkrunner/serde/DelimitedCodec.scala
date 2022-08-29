package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  MapperFeature,
  ObjectReader,
  ObjectWriter
}
import com.fasterxml.jackson.dataformat.csv.{
  CsvGenerator,
  CsvMapper,
  CsvParser
}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

trait DelimitedCodec {

  def getMapper: CsvMapper = CsvMapper
    .builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule)
    .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, false)
    .configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, false)
    .configure(CsvParser.Feature.TRIM_SPACES, true)
    .configure(CsvParser.Feature.SKIP_EMPTY_LINES, true)
    .configure(CsvParser.Feature.ALLOW_COMMENTS, true)
    .configure(CsvParser.Feature.EMPTY_STRING_AS_NULL, true)
    .configure(
      DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE,
      false
    )
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
    val schema = delimitedConfig
      .intoSchema(mapper.schemaFor(typeClass))
      .withUseHeader(false) // hardcoding this
    mapper.readerFor(typeClass).`with`(schema)
  }

}
