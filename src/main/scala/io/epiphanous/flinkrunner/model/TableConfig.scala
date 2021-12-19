package io.epiphanous.flinkrunner.model

import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, TableDescriptor}

import scala.collection.JavaConverters.propertiesAsScalaMapConverter

case class TableConfig(
    name: String,
    connector: TableConnector,
    format: TableFormat,
    columns: Map[String, String],
    options: Map[String, String]) {
  def register(tableEnv: StreamTableEnvironment): Unit =
    tableEnv.createTable(
      name,
      options
        .foldLeft(
          TableDescriptor
            .forConnector(connector.entryName)
            .format(format.entryName)
            .schema(
              columns
                .foldLeft(Schema.newBuilder()) { case (b, (n, t)) =>
                  b.column(n, t)
                }
                .build()
            )
        ) { case (d, (k, v)) => d.option(k, v) }
        .build()
    )

}

object TableConfig {
  def apply(name: String, config: FlinkConfig): TableConfig = {
    val p         = s"table.sources.$name"
    val connector =
      TableConnector.withNameInsensitive(config.getString(s"$p.connector"))
    val format    =
      TableFormat.withNameInsensitive(config.getString(s"$p.format"))
    val columns   = config.getProperties(s"$p.columns").asScala
    // TODO: fixup columns
    val options   = config.getProperties("$p.options").asScala
    // TODO: fixup options
    TableConfig(name, connector, format, columns.toMap, options.toMap)
  }
}
