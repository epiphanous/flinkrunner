package io.epiphanous.flinkrunner.model.source

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
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.{
  NoStoppingOffsetsInitializer,
  OffsetsInitializer
}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util.Properties
import scala.util.Try

case class KafkaSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Kafka)
    extends SourceConfig[ADT] {

  override val properties: Properties     = ConfigToProps.normalizeProps(
    config,
    pfx(),
    List("bootstrap.servers")
  )
  val topic: String                       = config.getString(pfx("topic"))
  val isKeyed: Boolean                    =
    config.getBooleanOpt(pfx("isKeyed")).getOrElse(true)
  val bootstrapServers: String            =
    properties.getProperty("bootstrap.servers")
  val bounded: Boolean                    =
    config.getBooleanOpt(pfx("bounded")).getOrElse(false)
  val startingOffsets: OffsetsInitializer =
    config.getStringOpt(pfx("starting.offset")) match {
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
  val stoppingOffsets: OffsetsInitializer =
    config.getStringOpt(pfx("stopping.offset")) match {
      case Some(o) if o.equalsIgnoreCase("latest")    =>
        OffsetsInitializer.latest()
      case Some(o) if o.equalsIgnoreCase("committed") =>
        OffsetsInitializer.committedOffsets()
      case Some(o) if o.matches("[0-9]+")             =>
        OffsetsInitializer.timestamp(o.toLong)
      case _                                          => new NoStoppingOffsetsInitializer()
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
      A <: GenericRecord](implicit fromKV: (Option[String], A) => E)
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

  def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: (Option[String], A) => E): DataStream[E] =
    _getSource(env, getAvroDeserializationSchema[E, A])

  def getSource[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] =
    _getSource(env, getDeserializationSchema)

  def _getSource[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment,
      deserializer: KafkaRecordDeserializationSchema[E]): DataStream[E] = {
    val ksb             = KafkaSource
      .builder[E]()
      .setTopics(topic)
      .setProperties(properties)
      .setStartingOffsets(startingOffsets)
      .setDeserializer(
        deserializer
      )
    val kafkaSrcBuilder =
      if (bounded)
        ksb.setBounded(stoppingOffsets)
      else ksb
    env
      .fromSource(
        kafkaSrcBuilder.build(),
        getWatermarkStrategy[E],
        s"wm:$label"
      )
      .uid(s"wm:$label")
  }
}
