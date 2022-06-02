package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.StreamFormatName
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema

import java.util.Properties

case class DelimitedConfig(
    columnSeparator: Char = ',',
    lineSeparator: String = "\n",
    quoteCharacter: Char = '"',
    escapeChar: Char = '\\'
) {
  def intoSchema(schema: CsvSchema): CsvSchema = {
    schema
      .withColumnSeparator(columnSeparator)
      .withLineSeparator(lineSeparator)
      .withQuoteChar(quoteCharacter)
      .withEscapeChar(escapeChar)
  }
}

object DelimitedConfig {
  val CSV = DelimitedConfig()
  val TSV = DelimitedConfig('\t')
  val PSV = DelimitedConfig('|')
  def get(
      format: StreamFormatName,
      properties: Properties): DelimitedConfig = {
    format match {
      case StreamFormatName.Tsv       => TSV
      case StreamFormatName.Psv       => PSV
      case StreamFormatName.Delimited =>
        DelimitedConfig(
          properties.getProperty("column.separator", ",").toCharArray.head,
          properties.getProperty("line.separator", "\n"),
          properties.getProperty("quote.character", "\"").toCharArray.head,
          properties.getProperty("escape.character", "\\").toCharArray.head
        )
      case _                          => CSV
    }
  }
}
