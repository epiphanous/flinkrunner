package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.AvroUtils.{
  isSpecific,
  schemaOf,
  toEmbeddedAvroInstance
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
  * @param schemaOpt
  *   optional avro schema string, which is required if A is GenericRecord
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
    schemaOpt: Option[String] = None
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val avroClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

  require(
    isSpecific(avroClass) || schemaOpt.nonEmpty,
    s"You must provide an avro record schema in the configuration of source `${sourceConfig.name}`" +
      " if you want to deserialize into a generic record type"
  )

  @transient lazy val deserializer
      : RegistryAvroDeserializationSchema[GenericRecord] =
    ConfluentRegistryAvroDeserializationSchema.forGeneric(
      schemaOf(avroClass, schemaOpt),
      sourceConfig.schemaRegistryConfig.url,
      sourceConfig.schemaRegistryConfig.props
    )

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {

    val headers = Option(record.headers())
      .map(_.asScala.map { h =>
        (h.key(), new String(h.value(), StandardCharsets.UTF_8))
      }.toMap)
      .getOrElse(Map.empty[String, String])

    val key = Option(record.key()).map(keyBytes =>
      new String(keyBytes, StandardCharsets.UTF_8)
    )

    deserializer.deserialize(record.value()) match {
      case a: GenericRecord        =>
        out.collect(
          toEmbeddedAvroInstance[E, A, ADT](
            a,
            avroClass,
            sourceConfig.config,
            key,
            headers
          )
        )
      case c if Option(c).nonEmpty =>
        throw new RuntimeException(
          s"deserialized value is an unexpected type of object: $c"
        )
    }
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
