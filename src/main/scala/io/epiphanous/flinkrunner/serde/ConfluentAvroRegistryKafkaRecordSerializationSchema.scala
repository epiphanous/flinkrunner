package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaRegistryClient
}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import io.epiphanous.flinkrunner.util.SinkDestinationNameUtils.RichSinkDestinationName
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders

import java.lang
import java.nio.charset.StandardCharsets

/** A serialization schema that uses a confluent avro schema registry
  * client to serialize an instance of a flink runner ADT into kafka. The
  * flink runner ADT class must also extend the EmbeddedAvroRecord trait.
  * @param sinkConfig
  *   the kafka sink config
  */
case class ConfluentAvroRegistryKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord,
    ADT <: FlinkEvent
](
    sinkConfig: KafkaSinkConfig[ADT],
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
) extends KafkaRecordSerializationSchema[E]
    with LazyLogging {

  @transient
  lazy val schemaRegistryClient: SchemaRegistryClient =
    schemaRegistryClientOpt.getOrElse(
      new CachedSchemaRegistryClient(
        sinkConfig.schemaRegistryConfig.url,
        sinkConfig.schemaRegistryConfig.cacheCapacity,
        sinkConfig.schemaRegistryConfig.props,
        sinkConfig.schemaRegistryConfig.headers
      )
    )

  @transient
  lazy val serializer: KafkaAvroSerializer = new KafkaAvroSerializer(
    schemaRegistryClient,
    sinkConfig.schemaRegistryConfig.props
  )

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val info = element.toKV(sinkConfig.config)

    val headers = new RecordHeaders()

    Option(info.headers).foreach { m =>
      m.foreach { case (hk, hv) =>
        headers.add(hk, hv.getBytes(StandardCharsets.UTF_8))
      }
    }

    val topic = sinkConfig.expandTemplate(info.record)

    val key = info.keyOpt.map(k => serializer.serialize(topic, k))

    logger.trace(
      s"serializing ${info.record.getSchema.getFullName} record ${element.$id} to topic <$topic> ${if (key.nonEmpty) "with key"
        else "without key"}, headers=${info.headers}"
    )

    val value = serializer.serialize(topic, info.record)

    new ProducerRecord(
      topic,
      null,
      element.$timestamp,
      key.orNull,
      value,
      headers
    )
  }

}
