package io.epiphanous.flinkrunner.serde

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent,
  SchemaRegistryConfig
}
import io.epiphanous.flinkrunner.util.AvroUtils.toEmbeddedAvroInstance
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.DeserializationSchema
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
  * @param preloaded
  *   an map of schemas to confluent avro deserializers (of type
  *   ConfluentRegistryAvroDeserializationSchema[GenericRecord]) to preload
  *   the cache for testing
  */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sourceConfig: KafkaSourceConfig[ADT]
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val avroClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

  @transient lazy val deserializer = new RegistryAvroDeserializationSchema(
    avroClass,
    null,
    new ConfluentSchemaCoderProvider(
      schemaRegistryUrl = sourceConfig.schemaRegistryConfig.url,
      schemaRegistryProps = sourceConfig.schemaRegistryConfig.props,
      subject = Some(s"${avroClass.getCanonicalName}-value")
    )
  )

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val topic   = sourceConfig.topic
    val headers = Option(record.headers())
      .map(_.asScala.map { h =>
        (h.key(), new String(h.value(), StandardCharsets.UTF_8))
      }.toMap)
      .getOrElse(Map.empty[String, String])
    val key     = Option(record.key()).map(keyBytes =>
      new String(keyBytes, StandardCharsets.UTF_8)
    )
    deserializer.deserialize(record.value()) match {
      case a: GenericRecord        =>
        out.collect(
          toEmbeddedAvroInstance[E, A, ADT](a, avroClass, key, headers)
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
