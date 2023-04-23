package io.epiphanous.flinkrunner.model

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.flink.CatalogLoader

import java.util
import scala.collection.JavaConverters._

case class IcebergCommonConfig(
    tableIdentifier: TableIdentifier,
    catalogLoader: CatalogLoader)

object IcebergCommonConfig {
  final val NESSIE_IMPL  = "org.apache.iceberg.nessie.NessieCatalog"
  final val ICEBERG_IMPL = "org.apache.iceberg.rest.RESTCatalog"

  def apply[ADT <: FlinkEvent](
      icebergSourceConfig: SourceOrSinkConfig[ADT])
      : IcebergCommonConfig = {
    val config     = icebergSourceConfig.config
    val pfx        = s => icebergSourceConfig.pfx(s)
    val hadoopConf = new Configuration()

    val namespace: Namespace =
      Namespace.of(
        config
          .getStringOpt(pfx("namespace"))
          .getOrElse("default")
          .split("\\."): _*
      )

    val tableName: String = config.getString(pfx("table"))

    val tableIdentifier: TableIdentifier =
      TableIdentifier.of(namespace, tableName)

    val (
      catalogName: String,
      catalogType: String,
      catalogProperties: util.Map[String, String]
    ) =
      (
        config
          .getStringOpt(pfx("catalog.name"))
          .getOrElse("default"),
        config.getStringOpt(pfx("catalog.type")).getOrElse("iceberg"),
        config
          .getProperties(pfx("catalog"))
          .asScala
          .filterKeys(k => !Seq("name", "type").contains(k))
          .foldLeft(Map.empty[String, String]) { case (m, kv) => m + kv }
          .asJava
      )

    val catalogLoader: CatalogLoader = catalogType.toLowerCase match {
      case "hive"   =>
        catalogProperties.put("type", "hive")
        CatalogLoader.hive(catalogName, hadoopConf, catalogProperties)
      case "hadoop" =>
        catalogProperties.put("type", "hadoop")
        CatalogLoader.hadoop(catalogName, hadoopConf, catalogProperties)
      case impl     =>
        CatalogLoader.custom(
          catalogName,
          catalogProperties,
          hadoopConf,
          impl match {
            case "iceberg" => ICEBERG_IMPL
            case "nessie"  => NESSIE_IMPL
            case _         => catalogType
          }
        )
    }

    IcebergCommonConfig(tableIdentifier, catalogLoader)
  }
}
