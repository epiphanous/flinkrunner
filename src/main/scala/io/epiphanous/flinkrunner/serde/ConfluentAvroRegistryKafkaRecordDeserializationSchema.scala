package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, EmbeddedAvroRecordInfo, FlinkEvent, SchemaRegistryConfig}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
  * @param schemaRegistryClientOpt
  *   an optional schema registry client
  */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord,
    ADT <: FlinkEvent
](
    sourceConfig: KafkaSourceConfig[ADT],
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  var valueDeserializer: KafkaAvroDeserializer       = _
  var keyDeserializer: Option[KafkaAvroDeserializer] = _

  override def open(
      context: DeserializationSchema.InitializationContext): Unit = {

    val schemaRegistryConfig: SchemaRegistryConfig =
      sourceConfig.schemaRegistryConfig

    val schemaRegistryClient: SchemaRegistryClient =
      schemaRegistryClientOpt.getOrElse(
        schemaRegistryConfig.getClient
      )

    valueDeserializer = new KafkaAvroDeserializer(
      schemaRegistryClient,
      schemaRegistryConfig.props
    )

    keyDeserializer = if (sourceConfig.isKeyed) {
      val ks = new KafkaAvroDeserializer(schemaRegistryClient)
      ks.configure(schemaRegistryConfig.props, true)
      Some(ks)
    } else None
  }

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val topic   = sourceConfig.topic
    val headers = Option(record.headers())
      .map(_.asScala.map { h =>
        (h.key(), new String(h.value(), StandardCharsets.UTF_8))
      }.toMap)
      .getOrElse(Map.empty[String, String])
    val key     =
      keyDeserializer.map(ds =>
        ds.deserialize(topic, record.key()).toString
      )
    valueDeserializer
      .deserialize(topic, record.value()) match {
      case a: GenericRecord        =>
        val obj = fromKV(
          EmbeddedAvroRecordInfo(a.asInstanceOf[A], key, headers)
        )
        logger.trace(
          s"deserializing ${a.getSchema.getFullName} record ${obj.$id} from $topic with key=$key, headers=$headers"
        )
        out.collect(obj)
      case c if Option(c).nonEmpty =>
        throw new RuntimeException(
          s"deserialized value is an unexpected type of object: $c"
        )
    }
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
