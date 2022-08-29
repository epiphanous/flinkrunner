package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.SupportedDatabase

import scala.collection.mutable

case class SqlDialect(
    product: SupportedDatabase,
    identQuoting: Quoting,
    literalQuoting: Quoting) {

  val maxIdentifierLength: Int = product match {
    case SupportedDatabase.Postgresql => 63
    case SupportedDatabase.Mysql      => 64
    case SupportedDatabase.Snowflake  => 64
    case SupportedDatabase.SqlServer  => 128
  }

  def filterIdentifierComponents(components: Seq[String]): Seq[String] =
    product match {
      case SupportedDatabase.Mysql if components.size > 2 =>
        components.take(1) ++ components.drop(2)
      case _                                              => components
    }

  def identifier(
      sb: mutable.StringBuilder,
      components: String*): mutable.StringBuilder = {
    val nonIgnoredComponents = filterIdentifierComponents(components)
    val cleanComponents      = nonIgnoredComponents
      .map(c =>
        c.replaceAll("[^-A-Za-z_0-9]", "")
          .strip()
          .take(maxIdentifierLength)
      )
    if (cleanComponents.toSet != nonIgnoredComponents.toSet)
      throw new RuntimeException(
        s"identifiers may only contain character [-A-Za-z_0-9]: (${components.mkString(", ")})"
      )

    sb.append(
      nonIgnoredComponents
        .map { comp =>
          identQuoting
            .quote(
              new mutable.StringBuilder,
              comp
            )
            .result()
        }
        .mkString(".")
    )
    sb
  }

  def literal(
      sb: mutable.StringBuilder,
      content: String): mutable.StringBuilder =
    literalQuoting.quote(sb, content)
}

object SqlDialect {
  def apply(product: SupportedDatabase): SqlDialect = SqlDialect(
    product,
    Quoting.ofIdentifiers(product),
    Quoting.ofLiterals(product)
  )
}
