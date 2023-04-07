package com.mdsol.streaming.util

import java.util.regex.Pattern

object RichString {

  private val camelCasePattern1: Pattern   =
    Pattern.compile("([A-Z]+)([A-Z][a-z])")
  private val camelCasePattern2: Pattern   =
    Pattern.compile("([a-z\\d])([A-Z])")
  private val camelCaseReplacement: String = "$1_$2"

  def splitCamel(string: String): Array[String] = {
    val first =
      camelCasePattern1.matcher(string).replaceAll(camelCaseReplacement)
    camelCasePattern2
      .matcher(first)
      .replaceAll(camelCaseReplacement)
      .split("_")
  }

  def camel2XCase(string: String, sep: String): String =
    splitCamel(string).mkString(sep).toLowerCase

  implicit class RichString(string: String) {

    def dotCase: String      = camel2XCase(string, ".").toLowerCase
    def upperDotCase: String = dotCase.toUpperCase

    def snakeCase: String      = camel2XCase(string, "_").toLowerCase
    def upperSnakeCase: String = snakeCase.toUpperCase

    def kebabCase: String      = camel2XCase(string, "-").toLowerCase
    def upperKebabCase: String = kebabCase.toUpperCase

  }

}
