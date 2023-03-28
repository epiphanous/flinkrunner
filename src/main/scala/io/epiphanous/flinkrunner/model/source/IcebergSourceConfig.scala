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

case class IcebergSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Iceberg

  val common: IcebergCommonConfig = IcebergCommonConfig(this)

  // TODO: add more config here

  override def getRowSource(
      env: StreamExecutionEnvironment): DataStream[RowData] = {
    val sourceBuilder = FlinkSource
      .forRowData()
      .env(env.getJavaEnv)
      .streaming(true)
      .tableLoader(
        TableLoader
          .fromCatalog(common.catalogLoader, common.tableIdentifier)
      )
    new DataStream[RowData](sourceBuilder.build())
  }

}
