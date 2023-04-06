package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.apache.iceberg.flink.sink.FlinkSink
import org.apache.iceberg.flink.{
  FlinkSchemaUtil,
  FlinkWriteOptions,
  TableLoader
}
import org.apache.iceberg.{PartitionSpec, Schema, Table}

import scala.collection.JavaConverters._
import scala.util.Try

/** An iceberg sink configuration.
  * @param name
  *   sink name
  * @param config
  *   sink configuration
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class IcebergSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT] {

  override def connector: FlinkConnectorName = FlinkConnectorName.Iceberg

  val icebergCommonConfig: IcebergCommonConfig = IcebergCommonConfig(this)

  val primaryKey: Seq[String] = config.getStringListOpt(pfx("primary.key"))

  val partitionSpecConfig: Seq[IcebergPartitionColumn] =
    config
      .getObjectList(pfx("partition.spec"))
      .map(_.toConfig)
      .map(IcebergPartitionColumn.apply)

  val writeFormat: String = config
    .getStringOpt(pfx("write.format"))
    .map(_.toLowerCase)
    .getOrElse("parquet")

  val writeParallelism: Int =
    config.getIntOpt("write.parallelism").getOrElse(2)

  /** Convert row type into a flink TableSchema. TableSchema is deprecated
    * in Flink, but the current Iceberg integration with Flink relies on
    * it.
    * @param rowType
    *   RowType - The type of Row objects flowing into the sink
    * @return
    *   TableSchema
    */
  def getFlinkTableSchema(rowType: RowType): TableSchema = {
    val tsb = rowType.getFields.asScala.foldLeft(TableSchema.builder()) {
      case (b, f) =>
        b.field(
          f.getName,
          TypeConversions.fromLogicalToDataType(f.getType)
        )
        b
    }
    if (primaryKey.nonEmpty) tsb.primaryKey(primaryKey: _*)
    tsb.build()
  }

  /** Given the table schema, try to ensure the target table exists in the
    * catalog
    *
    * TODO: this needs more work to handle altering table definition if it
    * does exist but doesn't match the configured specs
    *
    * @param flinkTableSchema
    *   a flink TableSchema instance
    * @return
    *   Try[Boolean] - true if we created the table, false otherwise
    */
  def maybeCreateTable(flinkTableSchema: TableSchema): Try[Table] = {
    logger.debug(s"enter maybeCreateTable $flinkTableSchema")
    val t = Try {
      val icebergSchema: Schema = FlinkSchemaUtil.convert(flinkTableSchema)
      logger.debug(icebergSchema.toString)
      val catalog               = icebergCommonConfig.catalogLoader.loadCatalog()
      logger.debug(catalog.toString)
      val ps                    =
        if (partitionSpecConfig.nonEmpty)
          partitionSpecConfig
            .foldLeft(PartitionSpec.builderFor(icebergSchema)) {
              case (psb, pc) =>
                pc.addToSpec(psb)
            }
            .build()
        else PartitionSpec.unpartitioned()
      logger.debug(ps.toString)
      logger.debug(icebergCommonConfig.tableIdentifier.toString)
      if (catalog.tableExists(icebergCommonConfig.tableIdentifier))
        catalog.loadTable(icebergCommonConfig.tableIdentifier)
      else
        catalog.createTable(
          icebergCommonConfig.tableIdentifier,
          icebergSchema,
          ps
        )
    }
    logger.debug(s"exit maybeCreateTable with $t")
    t
  }

  /** Add an iceberg row sink for the given avro data stream and row type
    *
    * @param rows
    *   a stream of rows
    * @param rowType
    *   configured or inferred from avro row type
    */
  override def _addRowSink(
      rows: DataStream[Row],
      rowType: RowType): Unit = {
    val flinkTableSchema = getFlinkTableSchema(rowType)
    maybeCreateTable(flinkTableSchema).fold(
      err =>
        throw new RuntimeException(
          s"Failed to create iceberg table ${icebergCommonConfig.tableIdentifier}",
          err
        ),
      table => logger.info(s"iceberg table $table ready")
    )
    FlinkSink
      .forRow(rows.javaStream, flinkTableSchema)
      .set(FlinkWriteOptions.WRITE_FORMAT.toString, writeFormat)
      .upsert(primaryKey.nonEmpty)
      .writeParallelism(writeParallelism)
      .tableLoader(
        TableLoader.fromCatalog(
          icebergCommonConfig.catalogLoader,
          icebergCommonConfig.tableIdentifier
        )
      )
      .append()

  }

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit = notImplementedError("addAvroSink")

  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    notImplementedError("addSink")
}
