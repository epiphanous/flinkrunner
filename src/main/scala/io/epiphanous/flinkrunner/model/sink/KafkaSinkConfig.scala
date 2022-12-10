package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  ConfluentAvroRegistryKafkaRecordSerializationSchema,
  JsonKafkaRecordSerializationSchema
}
import io.epiphanous.flinkrunner.util.ConfigToProps
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{
  KafkaRecordSerializationSchema,
  KafkaSink
}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

import java.time.Duration
import java.util.Properties

/** Kafka sink config.
  *
  * Configuration:
  *
  * @param name
  *   name of the sink
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class KafkaSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with LazyLogging {

  override val connector: FlinkConnectorName = FlinkConnectorName.Kafka

  override val properties: Properties = ConfigToProps.normalizeProps(
    config,
    pfx(),
    List("bootstrap.servers")
  )
  val bootstrapServers: String        =
    properties.getProperty("bootstrap.servers")

  val topic: String = config.getString(pfx("topic"))

  val isKeyed: Boolean = getFromEither(
    pfx(),
    Seq("keyed", "is.keyed"),
    config.getBooleanOpt
  ).getOrElse(false)

  def deliveryGuarantee: DeliveryGuarantee = config
    .getStringOpt(pfx("delivery.guarantee"))
    .map(s => s.toLowerCase.replaceAll("[^a-z]+", "-")) match {
    case Some("exactly-once") =>
      DeliveryGuarantee.EXACTLY_ONCE
    case Some("none")         =>
      DeliveryGuarantee.NONE
    case _                    => DeliveryGuarantee.AT_LEAST_ONCE
  }

  /** ensure transaction.timeout.ms is set */
  val transactionTimeoutMs: Long = {
    val tms = getFromEither(
      pfx(),
      Seq("transaction.timeout.ms", "tx.timeout.ms"),
      config.getLongOpt
    )
    val td  = getFromEither(
      pfx(),
      Seq("transaction.timeout", "tx.timeout"),
      config.getDurationOpt
    )
    val t   = tms.getOrElse(td.getOrElse(Duration.ofHours(2)).toMillis)
    properties.setProperty("transaction.timeout.ms", t.toString)
    t
  }

  val transactionalIdPrefix: String =
    getFromEither(
      pfx(),
      Seq(
        "transactional.id.prefix",
        "transactional.prefix",
        "transactional.id",
        "transaction.id.prefix",
        "transaction.prefix",
        "transaction.id",
        "tx.id.prefix",
        "tx.prefix",
        "tx.id"
      ),
      config.getStringOpt
    ).getOrElse(
      s"${config.jobName}.$name.tx.id"
    )

  val schemaRegistryConfig: SchemaRegistryConfig = SchemaRegistryConfig(
    isDeserializing = false,
    config
      .getObjectOption(pfx("schema.registry"))
  )

  val cacheConcurrencyLevel: Int =
    config.getIntOpt(pfx("cache.concurrency.level")).getOrElse(4)

  val cacheMaxSize: Long =
    config.getLongOpt(pfx("cache.max.size")).getOrElse(10000L)

  val cacheExpireAfter: Duration = config
    .getDurationOpt(pfx("cache.expire.after"))
    .getOrElse(Duration.ofHours(1))

  val cacheRecordStats: Boolean =
    config.getBooleanOpt(pfx("cache.record.stats")).getOrElse(true)

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

  override def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    dataStream.sinkTo(_getSink[E](getAvroSerializationSchema[E, A]))

  override def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    dataStream.sinkTo(_getSink[E](getSerializationSchema[E]))

  def _getSink[E <: ADT: TypeInformation](
      serializer: KafkaRecordSerializationSchema[E]): KafkaSink[E] =
    KafkaSink
      .builder()
      .setBootstrapServers(bootstrapServers)
      .setDeliveryGuarantee(deliveryGuarantee)
      .setTransactionalIdPrefix(transactionalIdPrefix)
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(serializer)
      .build()
}
