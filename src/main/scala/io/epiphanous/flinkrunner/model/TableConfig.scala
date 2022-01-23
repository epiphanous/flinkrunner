package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{TableDescriptor, Schema => TableSchema}
import org.apache.flink.table.types.logical.{LogicalType, MapType, RowType}

import scala.collection.JavaConverters.{
  collectionAsScalaIterableConverter,
  propertiesAsScalaMapConverter
}
import scala.collection.mutable
import scala.util.{Failure, Try}

case class TableConfig(
    name: String,
    connector: TableConnector,
    format: TableFormat,
    columns: Map[String, String],
    options: Map[String, String])
    extends LazyLogging {
  def register(tableEnv: StreamTableEnvironment): Unit =
    if (!tableEnv.listTables().contains(name)) {
      logger.debug(s"registering table $name with config $this")
      tableEnv.createTable(
        name,
        options
          .foldLeft(
            TableDescriptor
              .forConnector(connector.entryName)
              .format(format.entryName)
              .schema(
                columns
                  .foldLeft(TableSchema.newBuilder()) { case (b, (n, t)) =>
                    b.column(n, t)
                  }
                  .build()
              )
          ) { case (d, (k, v)) => d.option(k, v) }
          .build()
      )
    }

}

object TableConfig extends LazyLogging {
  val OPT_FROM_AVRO_SCHEMA = "from.avro.schema"
  val OPT_FORMAT           = "format"
  val OPT_VALUE_FORMAT     = s"value.$OPT_FORMAT"
  val OPT_KEY_FORMAT       = s"key.$OPT_FORMAT"
  val AVRO_CONFLUENT       = TableFormat.AvroConfluent.entryName
  val DEFAULT_FORMAT       = TableFormat.AvroConfluent
  val DEFAULT_CONNECTOR    = "kafka"
  def apply(
      name: String,
      config: FlinkConfig,
      schemaRegistryClient: Option[SchemaRegistryClient] = None)
      : TableConfig = {
    val p         = s"tables.$name"
    val connector =
      TableConnector.withNameInsensitive(
        config.getStringOpt(s"$p.connector").getOrElse(DEFAULT_CONNECTOR)
      )
    val format    =
      TableFormat.withNameInsensitive(
        config
          .getStringOpt(s"$p.format")
          .getOrElse(DEFAULT_FORMAT.entryName)
      )
    val columns   = config.getProperties(s"$p.columns").asScala
    val options   = config.getProperties(s"$p.options").asScala
    // make sure our connector is property formatted
    options += "connector" -> connector.entryName
    if (options.getFirst(Seq(OPT_VALUE_FORMAT, OPT_FORMAT)).isEmpty) {
      if (options.contains(OPT_KEY_FORMAT))
        options += OPT_VALUE_FORMAT -> format.entryName
      else {
        options += OPT_FORMAT -> format.entryName
      }
    }
    // maybe auto load from schema registry
    if (options.get(OPT_FROM_AVRO_SCHEMA).contains("true")) {
      options -= OPT_FROM_AVRO_SCHEMA
      if (
        options
          .getFirst(Seq(OPT_FORMAT, OPT_VALUE_FORMAT))
          .contains(AVRO_CONFLUENT)
      ) {
        schemaRegistryClient.foreach(c =>
          columns ++= loadFromSchema(c, name, options)
        )
      }
    }
    TableConfig(name, connector, format, columns.toMap, options.toMap)
  }

  /**
   * Use the provided schema registry client to load the subject schema,
   * inferred from provided options, and generate the columns as a map to
   * configure the table.
   *
   * @param schemaRegistryClient
   *   a provided schema registry client
   * @param tableName
   *   the name of the table to configure
   * @param options
   *   the options provided to configure the table
   * @return
   *   Map[String, String] of column names/types
   */
  def loadFromSchema(
      schemaRegistryClient: SchemaRegistryClient,
      tableName: String,
      options: mutable.Map[String, String]): Map[String, String] =
    (for {
      subject <- options.getFirst(
                   Seq(
                     s"value.$AVRO_CONFLUENT.subject",
                     s"$AVRO_CONFLUENT.subject"
                   )
                 ) |# { case None =>
                   logger.error(
                     s"no schema registry subject defined for table $tableName"
                   )
                 }
      schemaInfo <- (Try(
                      schemaRegistryClient.getLatestSchemaMetadata(subject)
                    ) |# { case Failure(exception: Exception) =>
                      logger.error(
                        s"failed to load table $tableName subject $subject from schema registry: ${exception.getMessage}"
                      )
                    }).toOption
      logicalType <-
        (Try(
          AvroSchemaConverter.convertToDataType(schemaInfo.getSchema)
        ) |# { case Failure(exception) =>
          logger.error(
            s"failed to convert table $tableName subject $subject avro schema to flink data type: ${exception.getMessage}"
          )
        }).map(_.getLogicalType).toOption
    } yield logicalType) match {
      case Some(rowType: RowType) =>
        // might need to nest at least one step deeper
        rowType.getFields.asScala
          .map(field =>
            (field.getName, field.getType.asSerializableString())
          )
          .toMap
      case Some(otherType)        =>
        logger.error(
          s"expected $tableName avro schema to be a ROW type, but got ${otherType.asSummaryString()}"
        )
        Map.empty[String, String]
      case _                      => Map.empty[String, String]
    }

  def flattenType(
      logicalType: LogicalType,
      prefix: String = ""): Map[String, String] = {
    logicalType match {
      case rowType: RowType => Map.empty[String, String]
      case mapType: MapType => Map.empty[String, String]
    }
  }
}
