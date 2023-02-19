package io.epiphanous.flinkrunner.model

import com.typesafe.config.Config
import io.epiphanous.flinkrunner.model.IcebergPartitionColumn.{
  BUCKET_PATTERN,
  TRUNCATE_PATTERN
}
import org.apache.iceberg.PartitionSpec

import scala.util.matching.Regex

case class IcebergPartitionColumn(name: String, transform: String) {
  def addToSpec(builder: PartitionSpec.Builder): PartitionSpec.Builder = {
    transform.toLowerCase match {
      case "identity"          => builder.identity(name)
      case "year"              => builder.year(name)
      case "month"             => builder.month(name)
      case "day"               => builder.day(name)
      case "hour"              => builder.hour(name)
      case TRUNCATE_PATTERN(w) => builder.truncate(name, w.toInt)
      case BUCKET_PATTERN(n)   => builder.bucket(name, n.toInt)
      case _                   =>
        throw new RuntimeException(
          s"invalid iceberg partition tranform '$transform'"
        )
    }
  }
}

object IcebergPartitionColumn {
  val TRUNCATE_PATTERN: Regex                       = "truncate\\[(\\d+)]".r
  val BUCKET_PATTERN: Regex                         = "bucket\\[(\\d+)]".r
  def apply(config: Config): IcebergPartitionColumn =
    IcebergPartitionColumn(
      config.getString("column"),
      config.getString("transform")
    )
}
