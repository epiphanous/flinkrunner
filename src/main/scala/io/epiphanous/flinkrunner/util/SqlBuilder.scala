package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.SupportedDatabase

import scala.collection.mutable

/** A small class to generate SQL statements that are properly quoted for
  * supported database products.
  * @param product
  *   a SupportedDatabase instance
  */
case class SqlBuilder(product: SupportedDatabase) {
  val dialect: SqlDialect        = SqlDialect(product)
  val sql: mutable.StringBuilder = new mutable.StringBuilder()

  /** Append raw values (a string, a character, a number, etc) to the
    * builder.
    * @param value
    *   the raw value to add to the builder (must have a .toString method)
    * @return
    *   SqlBuilder
    */
  def append(value: Any): SqlBuilder = {
    sql.append(value)
    this
  }

  /** Add an identifier to the builder, making sure it's properly quoted.
    * Multiple names may be provided to support qualifying identifiers with
    * database, schema, and table references.
    * @param names
    *   one or more identifiers or scope qualifiers
    * @return
    *   SqlBuilder
    */
  def identifier(names: String*): SqlBuilder = {
    dialect.identifier(sql, names: _*)
    this
  }

  /** Add a literal string to the builder, making sure it's properly
    * quoted.
    * @param lit
    *   the literal string
    * @return
    *   SqlBuilder
    */
  def literal(lit: String): SqlBuilder = {
    dialect.literal(sql, lit)
    this
  }

  /** Extract the current result of the builder. This may be called
    * multiple times without changing the state of the builder.
    *
    * @see
    *   [[clear]]
    * @see
    *   [[getSqlAndClear]]
    * @return
    *   String
    */
  def getSql: String = sql.result()

  /** Clear the builder.
    *
    * @see
    *   [[getSqlAndClear]]
    * @see
    *   [[getSql]]
    * @return
    *   SqlBuilder
    */
  def clear: SqlBuilder = {
    sql.clear()
    this
  }

  /** Extract the builder result and clear the builder.
    *
    * @see
    *   [[getSql]]
    * @see
    *   [[clear]]
    * @return
    *   String
    */
  def getSqlAndClear: String = {
    val result = sql.result()
    sql.clear()
    result
  }

}
