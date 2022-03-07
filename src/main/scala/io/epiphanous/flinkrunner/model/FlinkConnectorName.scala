package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Snakecase
import enumeratum.{Enum, EnumEntry}

sealed trait FlinkConnectorName extends EnumEntry with Snakecase

object FlinkConnectorName extends Enum[FlinkConnectorName] {
  val values = findValues

  case object Kinesis extends FlinkConnectorName

  case object Kafka extends FlinkConnectorName

  case object File extends FlinkConnectorName

  case object BulkFile extends FlinkConnectorName

  case object Socket extends FlinkConnectorName

  case object CassandraSink extends FlinkConnectorName

  case object ElasticsearchSink extends FlinkConnectorName

  case object Jdbc extends FlinkConnectorName

  case object Collection extends FlinkConnectorName

  case object RabbitMQ extends FlinkConnectorName

}
