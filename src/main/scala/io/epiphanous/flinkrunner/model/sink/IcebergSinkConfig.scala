package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.DistributionMode
import org.apache.iceberg.flink.sink.FlinkSink
import org.apache.iceberg.flink.{CatalogLoader, TableLoader}

import scala.collection.JavaConverters._

case class IcebergSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with LazyLogging {

  override def connector: FlinkConnectorName = FlinkConnectorName.Iceberg

  val catalogProperties: Map[String, String] = ???
  // type: iceberg
  // uri: nessie api endpoint
  // ref: nessie branch we want to use
  // warehouse: the path to the warehouse (s3://...)
  // authentication.type = (NONE, BEARER, AWS, BASIC)

  val catalogName = "catalog-name"
  val hadoopConf  = new Configuration()
  val catalogImpl = "org.apache.iceberg.nessie.NessieCatalog"

  val catalogLoader: CatalogLoader = CatalogLoader.custom(
    catalogName,
    catalogProperties.asJava,
    hadoopConf,
    catalogImpl
  )

  // create database, table if doesn't exist

  override def addRowSink(
      dataStream: DataStream[Row],
      rowType: RowType): Unit = {
    val tableSchema = {
      val tsb = rowType.getFields.asScala
        // NOTE: using deprecated TableSchema since iceberg sink constructor requires it
        // TODO: What about primary key and watermark specs?
        .foldLeft(new TableSchema.Builder()) { case (b, f) =>
          b.field(f.getName, DataTypes.of(f.getType))
        }
      tsb.primaryKey("pkname", Array("c1", "c2"))
//      tsb.watermark("rowtime", "expr", DataTypes.of(new LogicalType()))
      tsb.build()
    }
    FlinkSink
      .forRow(dataStream.javaStream, tableSchema)
      .distributionMode(DistributionMode.RANGE)
      .equalityFieldColumns(List("C1", "C2").asJava)
      .upsert(true)
      .writeParallelism(2)
//      .flinkConf()
//      .tableLoader(TableLoader.fromCatalog(catalogLoader, tableId))
      .append()
  }

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit =
    throw new RuntimeException("IcebergSink only supports TableStreamJobs")

  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    throw new RuntimeException("IcebergSink only supports TableStreamJobs")
}
