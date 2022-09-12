package io.epiphanous.flinkrunner.serde

/** A Json encoder configuration
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
    endOfLine: Option[String] = Some(System.lineSeparator()))
