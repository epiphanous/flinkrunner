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

  def identifier(
      sb: mutable.StringBuilder,
      components: String*): mutable.StringBuilder = {
    val cleanComponents = components.map(c =>
      c.replaceAll("[^-A-Za-z_0-9]", "")
        .strip()
        .take(maxIdentifierLength)
    )
    if (cleanComponents.toSet != components.toSet)
      throw new RuntimeException(
        s"identifiers may only contain character [-A-Za-z_0-9]: (${components.mkString(", ")})"
      )

    sb.append(
      components
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
