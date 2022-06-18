package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kafka
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  ConfluentAvroRegistryKafkaRecordSerializationSchema,
  JsonKafkaRecordSerializationSchema
}
import io.epiphanous.flinkrunner.util.ConfigToProps
import io.epiphanous.flinkrunner.util.ConfigToProps._
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{
  KafkaRecordSerializationSchema,
  KafkaSink
}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

import java.util.Properties
import scala.util.Try

case class KafkaSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = Kafka
) extends SinkConfig[ADT]
    with LazyLogging {

  override val properties: Properties = ConfigToProps.normalizeProps(
    config,
    pfx(),
    List("bootstrap.servers")
  )
  val bootstrapServers: String        =
    properties.getProperty("bootstrap.servers")

  val topic: String    = config.getString(pfx("topic"))
  val isKeyed: Boolean =
    config.getBooleanOpt(pfx("is.keyed")).getOrElse(true)

  def deliveryGuarantee: DeliveryGuarantee = config
    .getStringOpt(pfx("delivery.guarantee"))
    .map(s => s.toLowerCase.replaceAll("[^a-z]+", "-")) match {
    case Some("at-least-once") =>
      DeliveryGuarantee.AT_LEAST_ONCE
    case Some("none")          =>
      DeliveryGuarantee.NONE
    case _                     => DeliveryGuarantee.EXACTLY_ONCE
  }

  /** ensure transaction.timeout.ms is set */
  val transactionTimeoutMs: Long = {
    val t = properties.getProperty("transaction.timeout.ms", "60000")
    properties.setProperty("transaction.timeout.ms", t)
    t.toLong
  }

  val schemaRegistryConfig: SchemaRegistryConfig =
    config
      .getObjectOption(pfx("schema.registry"))
      .map { o =>
        val c             = o.toConfig
        val url           = c.getString("url")
        val cacheCapacity =
          Try(c.getInt("cache.capacity")).toOption.getOrElse(1000)
        val headers       =
          Try(c.getObject("headers")).toOption.asProperties.asJavaMap
        val props         =
          Try(c.getObject("props")).toOption.asProperties.asJavaMap
        SchemaRegistryConfig(
          url,
          cacheCapacity,
          props,
          headers
        )
      }
      .getOrElse(SchemaRegistryConfig())

  /** Return an confluent avro serialization schema */
  def getAvroSerializationSchema[
      E <: ADT with EmbeddedAvroRecord[A],
      A <: GenericRecord]: KafkaRecordSerializationSchema[E] = {
    new ConfluentAvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
      this
    )
  }

  /** Returns, by default, a json serialization schema */
  def getSerializationSchema[E <: ADT: TypeInformation]
      : KafkaRecordSerializationSchema[E] =
    new JsonKafkaRecordSerializationSchema[E, ADT](this)

  def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    dataStream.sinkTo(_getSink[E](getAvroSerializationSchema[E, A]))

  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    dataStream.sinkTo(_getSink[E](getSerializationSchema[E]))

  def _getSink[E <: ADT: TypeInformation](
      serializer: KafkaRecordSerializationSchema[E]): KafkaSink[E] =
    KafkaSink
      .builder()
      .setBootstrapServers(bootstrapServers)
      .setDeliverGuarantee(deliveryGuarantee)
      .setTransactionalIdPrefix(name)
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(serializer)
      .build()
}
