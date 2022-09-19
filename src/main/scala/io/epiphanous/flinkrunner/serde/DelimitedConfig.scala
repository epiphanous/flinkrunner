package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{FlinkConfig, StreamFormatName}
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither

/** A delimited file format configuration.
  * @param columnSeparator
  *   the column separator character (defaults to ',')
  * @param lineSeparator
  *   the line separator string (defaults to "\n")
  * @param quoteCharacter
  *   the quote character (defaults to '"')
  * @param escapeCharacter
  *   the escape character (defaults to '\')
  * @param useHeader
  *   true to emit a header line with column names (defaults to false);
  *   this will always be set to false for reading delimited files
  * @param columns
  *   a list of string names for the columns
  */
case class DelimitedConfig(
    columnSeparator: Char = ',',
    lineSeparator: String = "\n",
    quoteCharacter: Char = '"',
    escapeCharacter: Char = '\\',
    useHeader: Boolean = true,
    useQuotes: Boolean = false,
    columns: Seq[String] = Seq.empty
)

object DelimitedConfig {
  val CSV: DelimitedConfig = DelimitedConfig()
  val TSV: DelimitedConfig = DelimitedConfig('\t')
  val PSV: DelimitedConfig = DelimitedConfig('|')

  /** Produces a DelimitedConfig based on the request StreamFormatName,
    * properties and columns passed in.
    * @param format
    *   the StreamFormatName describing the requested configuration
    * @param prefix
    *   a configuration prefix
    * @param config
    *   flink config to pull config info from, relative to prefix
    * @param columns
    *   an optional list of column names (only required if the class being
    *   encoded is an avro GenericRecord)
    * @return
    *   DelimitedConfig
    */
  def get(
      format: StreamFormatName,
      prefix: String,
      config: FlinkConfig,
      columns: Seq[String] = Seq.empty
  ): DelimitedConfig = {

    val useQuotes: Option[Boolean] = getFromEither(
      prefix,
      Seq("uses.quotes", "use.quotes", "use.quote"),
      config.getBooleanOpt
    )

    val useHeader: Option[Boolean] = getFromEither(
      prefix,
      Seq("uses.header", "use.header", "use.headers"),
      config.getBooleanOpt
    )

    val colSep: Option[String] = getFromEither(
      prefix,
      Seq("column.separator", "col.sep"),
      config.getStringOpt
    )

    val lineSep: Option[String] = getFromEither(
      prefix,
      Seq("line.separator", "line.sep"),
      config.getStringOpt
    )

    val quoteChar: Option[String] = getFromEither(
      prefix,
      Seq("quote.character", "quote.char"),
      config.getStringOpt
    )

    val escapeChar: Option[String] = getFromEither(
      prefix,
      Seq("escape.character", "escape.char"),
      config.getStringOpt
    )

    val defaults = format match {
      case StreamFormatName.Tsv => TSV
      case StreamFormatName.Psv => PSV
      case _                    => CSV
    }

    DelimitedConfig(
      columnSeparator =
        colSep.map(_.head).getOrElse(defaults.columnSeparator),
      lineSeparator = lineSep.getOrElse(defaults.lineSeparator),
      quoteCharacter =
        quoteChar.map(_.head).getOrElse(defaults.quoteCharacter),
      escapeCharacter =
        escapeChar.map(_.head).getOrElse(defaults.escapeCharacter),
      useHeader = useHeader.getOrElse(defaults.useHeader),
      useQuotes = useQuotes.getOrElse(defaults.useQuotes),
      columns = columns
    )
  }
}
