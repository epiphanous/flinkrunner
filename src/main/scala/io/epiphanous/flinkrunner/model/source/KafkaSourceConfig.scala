package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  ConfluentAvroRegistryKafkaRecordDeserializationSchema,
  JsonKafkaRecordDeserializationSchema
}
import io.epiphanous.flinkrunner.util.ConfigToProps
import io.epiphanous.flinkrunner.util.ConfigToProps._
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util.Properties
import scala.util.Try

/** A source config for using a kafka as a source for a flink job. For
  * example, the following config can be used to read from a topic in kafka
  * that contains confluent avro encoded messages.
  * {{{
  *   source my-kafka-source {
  *     bootstrap.servers = "broker1:9092, broker2:9092, broker3:9092"
  *     topic = "my-avro-topic"
  *     is.keyed = true
  *     schema.registry.url = "http://schema-registry:8082"
  *   }
  * }}}
  *
  * Configuration options:
  *   - `connector`: `kafka` (required only if it can't be inferred from
  *     the source name)
  *   - `bootstrap.servers`: required list of kafka brokers (specified as a
  *     comma separated string)
  *   - `topic`: name of the topic (required; use
  *     <code>&lt;canonical-name&gt;</code> or
  *     <code>&lt;simple-name&gt;</code> to use the full namespaced or
  *     simple name of the event type as the topic name)
  *   - `keyed`: if true, the topic has a key (optional, defaults to false,
  *     and flinkrunner assumes kafka keys are strings)
  *   - `starting.offset`: `earliest`, `latest`, or timestamp in epoch
  *     millis (defaults to `earliest`)
  *   - `stopping.offset`: `latest`, `committed`, timestamp in epoch
  *     millis, or `none` (defaults to `none`)
  *   - `group.id`: consumer group id (defaults to
  *     <code>&lt;job-name&gt;.&lt;sink-name&gt;</code>)
  *   - `schema.registry`: optional confluent schema registry configuration
  *     - `url`: schema registry endpoint (required)
  *     - `cache.capacity`: size of schema cache in the registry client
  *       (defaults to 1000)
  *     - `headers`: key/value map of headers to pass to the schema
  *       registry (useful for auth)
  *     - `props`: other properties to pass to schema registry client
  *   - `config`: optional properties to pass to kafka client
  *
  * @param name
  *   name of the kafka source
  * @param runner
  *   flinkrunner instance
  * @tparam ADT
  *   Flinkrunner algebraic data type
  */
case class KafkaSourceConfig[ADT <: FlinkEvent](
    name: String,
    runner: FlinkRunner[ADT])
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Kafka

  override val properties: Properties = ConfigToProps.normalizeProps(
    config,
    pfx(),
    List("bootstrap.servers")
  )

  val topic: String = config.getString(pfx("topic"))

  val bootstrapServers: String =
    properties.getProperty("bootstrap.servers")

  val isKeyed: Boolean = getFromEither(
    pfx(),
    Seq("keyed", "is.keyed"),
    config.getBooleanOpt
  ).getOrElse(false)

  val startingOffsets: OffsetsInitializer =
    getFromEither(
      pfx(),
      Seq("starting.offset", "beginning.offset"),
      config.getStringOpt
    ) match {
      case Some(o) if o.equalsIgnoreCase("earliest") =>
        OffsetsInitializer.earliest()
      case Some(o) if o.equalsIgnoreCase("latest")   =>
        OffsetsInitializer.latest()
      case Some(o) if o.matches("[0-9]+")            =>
        OffsetsInitializer.timestamp(o.toLong)
      case _                                         =>
        OffsetsInitializer.committedOffsets(
          OffsetResetStrategy.EARLIEST
        )
    }

  val stoppingOffsets: Option[OffsetsInitializer] =
    getFromEither(
      pfx(),
      Seq("stopping.offset", "ending.offset"),
      config.getStringOpt
    ) match {
      case Some(o) if o.equalsIgnoreCase("latest")    =>
        Some(OffsetsInitializer.latest())
      case Some(o) if o.equalsIgnoreCase("committed") =>
        Some(OffsetsInitializer.committedOffsets())
      case Some(o) if o.matches("[0-9]+")             =>
        Some(OffsetsInitializer.timestamp(o.toLong))
      case _                                          => None
    }

  val groupId: String = config
    .getStringOpt(pfx("group.id"))
    .getOrElse(s"${config.jobName}.$name")

  val schemaRegistryConfig: SchemaRegistryConfig =
    getFromEither(pfx(), Seq("schema.registry"), config.getObjectOption)
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

  /** Returns a confluent avro registry aware deserialization schema for
    * kafka.
    *
    * @param fromKV
    *   an implicit method that creates an event from an optional key and
    *   avro record value
    * @tparam E
    *   the stream element type (must mix in EmbeddedAvroRecord[A] trait)
    * @tparam A
    *   the avro record type
    * @return
    *   KafkaRecordDeserializationSchema[E]
    */
  def getAvroDeserializationSchema[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : KafkaRecordDeserializationSchema[E] = {
    new ConfluentAvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      this
    )
  }

  /** Return a deserialization schema for kafka. This defaults to return a
    * JSON deserialization schema.
    *
    * @tparam E
    *   the event type
    * @return
    *   KafkaRecordDeserializationSchema[E]
    */
  def getDeserializationSchema[E <: ADT: TypeInformation]
      : KafkaRecordDeserializationSchema[E] =
    new JsonKafkaRecordDeserializationSchema[E, ADT](this)

  override def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Right(_getSource(getAvroDeserializationSchema[E, A]))

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Right(_getSource(getDeserializationSchema))

  def _getSource[E <: ADT: TypeInformation](
      deserializer: KafkaRecordDeserializationSchema[E])
      : KafkaSource[E] = {
    val ksb = KafkaSource
      .builder[E]()
      .setTopics(topic)
      .setGroupId(groupId)
      .setProperties(properties)
      .setStartingOffsets(startingOffsets)
      .setDeserializer(
        deserializer
      )
    stoppingOffsets.map(ksb.setBounded).getOrElse(ksb).build()
  }
}
