package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.csv.{CsvGenerator, CsvMapper, CsvParser, CsvSchema}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.pjfanning.jackson.reflect.ScalaReflectExtensions
import org.apache.avro.generic.GenericRecord

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

case class Codec[E](
    typeClass: Class[E],
    jsonConfig: JsonConfig = JsonConfig(),
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV) {

  object CsvMapperScalaReflectExtensions {
    def ::(mapper: CsvMapper) = new Mixin(mapper)

    final class Mixin private[CsvMapperScalaReflectExtensions] (
        mapper: CsvMapper)
        extends CsvMapper(mapper)
        with ScalaReflectExtensions
  }

  lazy val isAvro: Boolean =
    classOf[GenericRecord].isAssignableFrom(typeClass)

  lazy val avroModule: SimpleModule =
    new SimpleModule().addSerializer(new AvroJsonSerializer)

  lazy val jsonMapper: JsonMapper = {
    val mapper = JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .addModule(new JavaTimeModule)
      .configure(
        MapperFeature.SORT_PROPERTIES_ALPHABETICALLY,
        jsonConfig.sortKeys
      )
      .configure(
        SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,
        jsonConfig.sortKeys
      )
      .configure(SerializationFeature.INDENT_OUTPUT, jsonConfig.pretty)
      .configure(
        DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE,
        false
      )
    (if (isAvro) mapper.addModule(avroModule) else mapper)
      .build() :: ScalaReflectExtensions
  }

  lazy val jsonWriter: ObjectWriter = jsonMapper.writerFor(typeClass)

  lazy val jsonReader: ObjectReader = jsonMapper.readerFor(typeClass)

  lazy val csvMapper: CsvMapper = {
    val builder = CsvMapper
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
      .configure(
        DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS,
        false
      )
    (if (isAvro) builder.addModule(avroModule) else builder)
      .build() :: CsvMapperScalaReflectExtensions
  }

  lazy val csvSchema: CsvSchema = {
    val start             = csvMapper.schemaFor(typeClass)
    val updatedWithConfig = (if (isAvro) {
                               val columns = start
                                 .iterator()
                                 .asScala
                                 .toList
                                 .filterNot(c =>
                                   c.hasName("schema") || c.hasName(
                                     "specificData"
                                   )
                                 )
                                 .asJava
                               start
                                 .withoutColumns()
                                 .rebuild()
                                 .addColumns(columns)
                                 .build()
                             } else start)
      .withColumnSeparator(delimitedConfig.columnSeparator)
      .withLineSeparator(delimitedConfig.lineSeparator)
      .withEscapeChar(delimitedConfig.escapeCharacter)
      .withUseHeader(false) // delimited header use handled in encoder
    if (delimitedConfig.useQuotes)
      updatedWithConfig.withQuoteChar(delimitedConfig.quoteCharacter)
    else updatedWithConfig.withoutQuoteChar()
  }

  lazy val csvHeader: Array[Byte] = csvSchema
    .iterator()
    .asScala
    .map(_.getName)
    .toList
    .mkString(
      "",
      delimitedConfig.columnSeparator.toString,
      delimitedConfig.lineSeparator
    )
    .getBytes(StandardCharsets.UTF_8)

  def maybeWriteHeader(stream: OutputStream): Unit =
    if (delimitedConfig.useHeader) stream.write(csvHeader)

  lazy val csvWriter: ObjectWriter = csvMapper.writer(csvSchema)

  lazy val csvReader: ObjectReader =
    csvMapper.readerFor(typeClass).`with`(csvSchema)
}
