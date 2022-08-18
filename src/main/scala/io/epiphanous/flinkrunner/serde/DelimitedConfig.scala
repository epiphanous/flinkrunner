package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.epiphanous.flinkrunner.model.StreamFormatName

import java.util.Properties

/** A delimited file format configuration.
  * @param columnSeparator
  *   the column separator character (defaults to ',')
  * @param lineSeparator
  *   the line separator string (defaults to "\n")
  * @param quoteCharacter
  *   the quote character (defaults to '"')
  * @param escapeChar
  *   the escape character (defaults to '\')
  * @param useHeader
  *   true to emit a header line with column names (defaults to true)
  * @param columns
  *   a list of string names for the columns
  */
case class DelimitedConfig(
    columnSeparator: Char = ',',
    lineSeparator: String = "\n",
    quoteCharacter: Char = '"',
    escapeChar: Char = '\\',
    useHeader: Boolean = true,
    useQuotes: Boolean = false,
    columns: List[String] = List.empty
) {

  /** Creates a new CsvSchema object based on the DelimitedConfig settings.
    *
    * Note: If DelimitedConfig has a non-empty column list, any existing
    * columns in the start schema will be replaced.
    *
    * @param start
    *   a starting schema that we apply our settings into
    * @return
    *   updated CsvSchema
    */
  def intoSchema(start: CsvSchema): CsvSchema = {
    val csvSchema = {
      val s = start
        .withColumnSeparator(columnSeparator)
        .withLineSeparator(lineSeparator)
        .withEscapeChar(escapeChar)
        .withUseHeader(useHeader)
      if (useQuotes) s.withQuoteChar(quoteCharacter)
      else s.withoutQuoteChar()
    }
    if (columns.isEmpty) csvSchema
    else {
      columns
        .foldLeft(csvSchema.withoutColumns().rebuild())((b, f) =>
          b.addColumn(f)
        )
        .build()
    }
  }
}

object DelimitedConfig {
  val CSV = DelimitedConfig()
  val TSV = DelimitedConfig('\t')
  val PSV = DelimitedConfig('|')

  /** Produces a DelimitedConfig based on the request StreamFormatName,
    * properties and columns passed in.
    * @param format
    *   the StreamFormatName describing the requested configuration
    * @param properties
    *   delimited configuration as a set of properties (usually taken from
    *   the FileSink configuration)
    * @param columns
    *   an optional list of column names (only required if the class being
    *   encoded is an avro GenericRecord)
    * @return
    *   DelimitedConfig
    */
  def get(
      format: StreamFormatName,
      properties: Properties,
      columns: List[String] = List.empty): DelimitedConfig = {
    val useHeader = properties.getProperty("use.header", "true").toBoolean
    val useQuotes = properties.getProperty("use.quotes", "false").toBoolean
    format match {
      case StreamFormatName.Tsv       =>
        TSV.copy(
          useHeader = useHeader,
          useQuotes = useQuotes,
          columns = columns
        )
      case StreamFormatName.Psv       =>
        PSV.copy(
          useHeader = useHeader,
          useQuotes = useQuotes,
          columns = columns
        )
      case StreamFormatName.Delimited =>
        DelimitedConfig(
          properties.getProperty("column.separator", ",").toCharArray.head,
          properties.getProperty("line.separator", "\n"),
          properties.getProperty("quote.character", "\"").toCharArray.head,
          properties
            .getProperty("escape.character", "\\")
            .toCharArray
            .head,
          useHeader,
          useQuotes,
          columns
        )
      case _                          =>
        CSV.copy(
          useHeader = useHeader,
          useQuotes = useQuotes,
          columns = columns
        )
    }
  }
}
