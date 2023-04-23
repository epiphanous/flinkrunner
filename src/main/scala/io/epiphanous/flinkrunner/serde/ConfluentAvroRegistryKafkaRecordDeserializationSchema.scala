package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaRegistryClient
}
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.model.KafkaInfoHeader._
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.AvroUtils.isSpecific
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.util.Try

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
  * @param schemaOpt
  *   optional avro schema string, which is required if A is GenericRecord
  * @param schemaRegistryClientOpt
  *   optional schema registry client (useful for testing)
  * @tparam E
  *   event type being deserialized, with an embedded avro record
  * @tparam A
  *   avro record type embedded within E
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sourceConfig: KafkaSourceConfig[ADT],
    schemaOpt: Option[String] = None,
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val avroClass: Class[A]          = implicitly[TypeInformation[A]].getTypeClass
  val avroClassIsSpecific: Boolean = isSpecific(avroClass)

  require(
    avroClassIsSpecific || schemaOpt.nonEmpty,
    s"You must provide an avro record schema in the configuration of source `${sourceConfig.name}`" +
      " if you want to deserialize into a generic record type"
  )

  @transient
  lazy val schemaRegistryClient: SchemaRegistryClient =
    schemaRegistryClientOpt.getOrElse(
      new CachedSchemaRegistryClient(
        sourceConfig.schemaRegistryConfig.url,
        sourceConfig.schemaRegistryConfig.cacheCapacity,
        sourceConfig.schemaRegistryConfig.props,
        sourceConfig.schemaRegistryConfig.headers
      )
    )

  @transient
  lazy val deserializerProps: util.Map[String, String] =
    new KafkaAvroDeserializerConfig(
      sourceConfig.schemaRegistryConfig.props
    ).originalsStrings()

  /** The deserializer used for keys.
    *
    * Note: This forces keys to be deserialized as generic records and then
    * will call their toString() method to convert them to a string, since
    * we require all keys to be strings.
    */
  @transient
  lazy val keyDeserializer: KafkaAvroDeserializer = {
    val kkad = new KafkaAvroDeserializer(schemaRegistryClient)
    val p    = new util.HashMap[String, String]()
    p.putAll(deserializerProps)
    // NOTE: we deserialize keys as generic records and then toString the result
    p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false")
    kkad.configure(p, true)
    kkad
  }

  /** The deserializer used for values.
    *
    * Note: This forces values to be deserialized as specific records if A
    * is a specific record class (and forces deserialization as generic
    * records if A == GenericRecord)
    */
  @transient
  lazy val valueDeserializer: KafkaAvroDeserializer = {
    val vkad = new KafkaAvroDeserializer(schemaRegistryClient)
    val p    = new util.HashMap[String, String]()
    p.putAll(deserializerProps)
    // ensure we use the right setting for specific
    p.put(
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
      if (avroClassIsSpecific) "true" else "false"
    )
    vkad.configure(p, false)
    vkad
  }

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {

    /** extract headers */
    val headers = Option(record.headers())
      .map(_.asScala.map { h =>
        (h.key(), new String(h.value(), StandardCharsets.UTF_8))
      }.toMap)
      .getOrElse(Map.empty[String, String]) ++ Map(
      headerName(SerializedValueSize) -> record
        .serializedValueSize()
        .toString,
      headerName(SerializedKeySize)   -> record
        .serializedKeySize()
        .toString,
      headerName(Offset)              -> record.offset().toString,
      headerName(Partition)           -> record.partition().toString,
      headerName(Timestamp)           -> record.timestamp().toString,
      headerName(TimestampType)       -> record
        .timestampType()
        .name(),
      headerName(Topic)               -> record.topic()
    )

    // deserialize the key
    val keyOpt =
      Try(
        keyDeserializer.deserialize(
          record.topic(),
          record.key()
        )
      )
        .fold(
          error => {
            logger.error(
              s"failed to deserialize kafka message key (${record
                  .serializedKeySize()} bytes) from topic ${record.topic()}",
              error
            )
            None
          },
          k =>
            Some(
              k.toString
            ) // if result not already a string, convert it to a string
        )

    Try(
      valueDeserializer.deserialize(record.topic(), record.value())
    )
      .fold(
        error =>
          logger.error(
            s"failed to deserialize kafka message value (${record
                .serializedValueSize()} bytes) to a ${avroClass.getCanonicalName}",
            error
          ),
        {
          case rec: A     =>
            val event = fromKV(
              EmbeddedAvroRecordInfo(
                rec,
                sourceConfig.config,
                keyOpt,
                headers
              )
            )
            out.collect(event)
          case unexpected =>
            logger.error(
              s"Expected deserialized kafka message value of type ${avroClass.getCanonicalName}, but got [$unexpected]"
            )
        }
      )
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
