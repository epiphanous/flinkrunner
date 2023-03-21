package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  IcebergCommonConfig
}
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.table.data.RowData
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.source.FlinkSource

import java.time.Duration

case class IcebergSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Iceberg

  val common: IcebergCommonConfig = IcebergCommonConfig(this)

  val asOfTimestampOpt: Option[Long] =
    config.getLongOpt(pfx("as.of.timestamp"))

  val monitorInterval: Duration = config
    .getDurationOpt(pfx("monitor.interval"))
    .getOrElse(Duration.ofMillis(30000))

  // TODO: add more config here

  override def getRowSource(
      env: StreamExecutionEnvironment): DataStream[RowData] = {
    val sourceBuilder = FlinkSource
      .forRowData()
      .env(env.getJavaEnv)
      .streaming(true)
      .monitorInterval(monitorInterval)
      .tableLoader(
        TableLoader
          .fromCatalog(common.catalogLoader, common.tableIdentifier)
      )
    asOfTimestampOpt.foreach(ts => sourceBuilder.asOfTimestamp(ts))
    new DataStream(sourceBuilder.build())
  }

}
