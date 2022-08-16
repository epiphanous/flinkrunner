package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.epiphanous.flinkrunner.model.StreamFormatName

import java.util.Properties

case class DelimitedConfig(
    columnSeparator: Char = ',',
    lineSeparator: String = "\n",
    quoteCharacter: Char = '"',
    escapeChar: Char = '\\',
    useHeader: Boolean = true
) {
  def intoSchema(schema: CsvSchema): CsvSchema = {
    schema
      .withColumnSeparator(columnSeparator)
      .withLineSeparator(lineSeparator)
      .withQuoteChar(quoteCharacter)
      .withEscapeChar(escapeChar)
      .withUseHeader(useHeader)
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
