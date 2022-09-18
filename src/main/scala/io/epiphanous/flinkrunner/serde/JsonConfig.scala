package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.FlinkConfig
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither

/** A Json encoder configuration
  *
  * @param pretty
  *   true to indent the output with whitespace (default false)
  * @param sortKeys
  *   true to lexicographically sort json keys (default false)
  * @param endOfLine
  *   an optional end of line string (defaults to system line separator)
  */
case class JsonConfig(
    pretty: Boolean = false,
    sortKeys: Boolean = false,
    endOfLine: String = System.lineSeparator())

object JsonConfig {
  def apply(prefix: String, config: FlinkConfig): JsonConfig = {
    val defaults  = JsonConfig()
    val pretty    = getFromEither(
      prefix,
      Seq("pretty", "pretty.print"),
      config.getBooleanOpt
    ).getOrElse(defaults.pretty)
    val sortKeys  =
      getFromEither(prefix, Seq("sort.keys"), config.getBooleanOpt)
        .getOrElse(defaults.sortKeys)
    val endOfLine = getFromEither(
      prefix,
      Seq("eol", "line.separator", "line.ending", "line.sep"),
      config.getStringOpt
    ).getOrElse(defaults.endOfLine)
    JsonConfig(pretty, sortKeys, endOfLine)
  }
}
