package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.{
  QuotingEscapeStrategy,
  SupportedDatabase
}
import io.epiphanous.flinkrunner.util.Quoting.SINGLE_QUOTE

import scala.collection.mutable

case class Quoting(
    start: String = SINGLE_QUOTE,
    end: String = SINGLE_QUOTE,
    escapeStrategy: QuotingEscapeStrategy,
    escapeChar: String = "\\") {
  def quote(
      sb: mutable.StringBuilder,
      content: String): mutable.StringBuilder = {
    sb.append(start)
    sb.append(escape(content))
    sb.append(end)
    sb
  }
  def escape(content: String): String = escapeStrategy match {
    case QuotingEscapeStrategy.EscapeChar =>
      content.replaceAll(end, escapeChar + end)
    case QuotingEscapeStrategy.Doubling   =>
      content.replaceAll(end, end + end)
    case QuotingEscapeStrategy.SqlServer  =>
      content.replaceAll(start, start + end)
  }
}

object Quoting {
  final val DOUBLE_QUOTE  = "\""
  final val SINGLE_QUOTE  = "'"
  final val BACK_TICK     = "`"
  final val OPEN_BRACKET  = "["
  final val CLOSE_BRACKET = "]"

  def ofIdentifiers(product: SupportedDatabase): Quoting =
    product match {
      case SupportedDatabase.Postgresql =>
        Quoting(
          DOUBLE_QUOTE,
          DOUBLE_QUOTE,
          QuotingEscapeStrategy.EscapeChar
        )
      case SupportedDatabase.Mysql      =>
        Quoting(BACK_TICK, BACK_TICK, QuotingEscapeStrategy.EscapeChar)
      case SupportedDatabase.Snowflake  =>
        Quoting(
          DOUBLE_QUOTE,
          DOUBLE_QUOTE,
          QuotingEscapeStrategy.EscapeChar
        )
      case SupportedDatabase.SqlServer  =>
        Quoting(
          OPEN_BRACKET,
          CLOSE_BRACKET,
          QuotingEscapeStrategy.SqlServer
        )
    }

  def ofLiterals(product: SupportedDatabase): Quoting = product match {
    case SupportedDatabase.Postgresql =>
      Quoting(SINGLE_QUOTE, SINGLE_QUOTE, QuotingEscapeStrategy.Doubling)
    case SupportedDatabase.Mysql      =>
      Quoting(SINGLE_QUOTE, SINGLE_QUOTE, QuotingEscapeStrategy.EscapeChar)
    case SupportedDatabase.Snowflake  =>
      Quoting(SINGLE_QUOTE, SINGLE_QUOTE, QuotingEscapeStrategy.EscapeChar)
    case SupportedDatabase.SqlServer  =>
      Quoting(SINGLE_QUOTE, SINGLE_QUOTE, QuotingEscapeStrategy.Doubling)
  }

}
