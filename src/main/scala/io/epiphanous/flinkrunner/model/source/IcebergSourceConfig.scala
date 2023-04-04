package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  IcebergCommonConfig
}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.table.data.RowData
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory
import org.apache.iceberg.flink.source.{
  IcebergSource,
  StreamingStartingStrategy
}

import java.time.Duration

case class IcebergSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Iceberg

  val common: IcebergCommonConfig = IcebergCommonConfig(this)

  val streamingOpt: Option[Boolean] = config.getBooleanOpt("streaming")
  val batchOpt: Option[Boolean]     = config.getBooleanOpt(pfx("batch"))
  val batch: Boolean                =
    streamingOpt.contains(false) || batchOpt.contains(true)

  val streamingStartingStrategy: Option[StreamingStartingStrategy] = config
    .getStringOpt(pfx("start.strategy"))
    .map(_.toLowerCase.replaceAll("[^a-zA-Z]+", "_"))
    .map {
      case "full" | "table_scan_then_incremental"                       =>
        StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL
      case "latest" | "incremental_from_latest_snapshot"                =>
        StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT
      case "earliest" | "incremental_from_earliest_snapshot"            =>
        StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT
      case "id" | "snapshot_id" | "incremental_from_snapshot_id"        =>
        StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID
      case "ts" | "snapshot_ts" | "incremental_from_snapshot_timestamp" =>
        StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP
      case ss                                                           =>
        throw new RuntimeException(
          s"Unknown start.strategy $ss for iceberg source $name"
        )
    }

  val startSnapshotId: Option[Long] =
    config.getLongOpt(pfx("start.snapshot.id"))

  val startSnapshotTs: Option[Long] =
    config.getLongOpt(pfx("start.snapshot.timestamp"))

  val monitoringInterval: Option[Duration] =
    config.getDurationOpt(pfx("monitor.interval"))

  if (
    streamingStartingStrategy.contains(
      StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID
    )
    && startSnapshotId.isEmpty
  )
    throw new RuntimeException(
      s"Missing configuration for start.snapshot.id for iceberg source $name"
    )

  if (
    streamingStartingStrategy.contains(
      StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP
    )
    && startSnapshotTs.isEmpty
  )
    throw new RuntimeException(
      s"Missing configuration for start.snapshot.timestamp for iceberg source $name"
    )

  override def getRowSource(
      env: StreamExecutionEnvironment): DataStream[RowData] = {
    val sourceBuilder = IcebergSource
      .forRowData()
      .assignerFactory(new SimpleSplitAssignerFactory())
      .streaming(!batch)
      .tableLoader(
        TableLoader
          .fromCatalog(common.catalogLoader, common.tableIdentifier)
      )
    logger.debug(s"setting properties: $propertiesMap")
    sourceBuilder.setAll(propertiesMap)
    if (!batch) {
      streamingStartingStrategy.foreach(ss =>
        sourceBuilder.streamingStartingStrategy(ss)
      )
      startSnapshotId.foreach(id => sourceBuilder.startSnapshotId(id))
      startSnapshotTs.foreach(ts =>
        sourceBuilder.startSnapshotTimestamp(ts)
      )
      monitoringInterval.foreach(mi => sourceBuilder.monitorInterval(mi))
    }
    env
      .fromSource(
        sourceBuilder.build(),
        WatermarkStrategy.noWatermarks(),
        name
      )
      .uid(uid)
      .setParallelism(parallelism)
  }

}
