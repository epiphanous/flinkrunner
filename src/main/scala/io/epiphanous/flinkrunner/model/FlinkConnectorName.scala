package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Snakecase
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait FlinkConnectorName extends EnumEntry with Snakecase

object FlinkConnectorName extends Enum[FlinkConnectorName] {
  val values: immutable.IndexedSeq[FlinkConnectorName] = findValues

  case object Hybrid extends FlinkConnectorName

  case object Kinesis extends FlinkConnectorName

  case object Kafka extends FlinkConnectorName

  case object File extends FlinkConnectorName

  case object Socket extends FlinkConnectorName

  case object CassandraSink extends FlinkConnectorName

  case object ElasticsearchSink extends FlinkConnectorName

  case object Jdbc extends FlinkConnectorName

  case object RabbitMQ extends FlinkConnectorName

  case object Generator extends FlinkConnectorName

  val sources: immutable.Seq[FlinkConnectorName] =
    values diff IndexedSeq(CassandraSink, ElasticsearchSink)
  val sinks: immutable.Seq[FlinkConnectorName]   =
    values diff IndexedSeq(Hybrid, Generator)

  def fromSourceName(
      sourceName: String,
      jobName: String,
      connectorNameOpt: Option[String] = None,
      defaultOpt: Option[FlinkConnectorName] = None): FlinkConnectorName =
    fromName("source", sourceName, jobName, connectorNameOpt, defaultOpt)

  def fromSinkName(
      sinkName: String,
      jobName: String,
      connectorNameOpt: Option[String] = None,
      defaultOpt: Option[FlinkConnectorName] = None): FlinkConnectorName =
    fromName("sink", sinkName, jobName, connectorNameOpt, defaultOpt)

  def fromName(
      sourceOrSink: String,
      sourceOrSinkName: String,
      jobName: String,
      connectorNameOpt: Option[String] = None,
      defaultOpt: Option[FlinkConnectorName] = None)
      : FlinkConnectorName = {
    val sourceOrSinkID = s"$sourceOrSinkName $sourceOrSink in job $jobName"
    val connector      = (connectorNameOpt match {
      case Some(connectorName) => withNameInsensitiveOption(connectorName)
      case None                =>
        val lcName         = sourceOrSinkName.toLowerCase
        val lcNameSuffixed = s"${lcName}_$sourceOrSink"
        values.find { c =>
          Seq(lcName, lcNameSuffixed).exists(
            _.contains(c.entryName.toLowerCase)
          )
        }
    }) match {
      case Some(c) => c
      case None    =>
        defaultOpt match {
          case Some(c) => c
          case None    =>
            throw new RuntimeException(
              s"No valid connector type found for $sourceOrSinkID. Please set the connector type in the $sourceOrSink configuration."
            )
        }
    }
    sourceOrSink match {
      case "source" if sources.contains(connector) => connector
      case "sink" if sinks.contains(connector)     => connector
      case _                                       =>
        throw new RuntimeException(
          s"${connector.entryName} is an invalid connector for $sourceOrSinkID"
        )
    }
  }

}
