package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkEvent,
  SchemaRegistryConfig
}
import io.epiphanous.flinkrunner.util.SinkDestinationNameUtils.RichSinkDestinationName
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

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

  val topic: String       = sinkConfig.topic
  val config: FlinkConfig = sinkConfig.config

  val schemaRegistryConfig: SchemaRegistryConfig =
    sinkConfig.schemaRegistryConfig

  lazy val schemaRegistryClient: SchemaRegistryClient =
    schemaRegistryClientOpt.getOrElse(
      schemaRegistryConfig.getClient(config.mockEdges)
    )

  /** value serializer */
  lazy val valueSerializer = new KafkaAvroSerializer(
    schemaRegistryClient,
    schemaRegistryConfig.props
  )

  /** add the key serializer if needed */
  lazy val keySerializer: Option[KafkaAvroSerializer] =
    if (sinkConfig.isKeyed) {
      val ks = new KafkaAvroSerializer(schemaRegistryClient)
      ks.configure(schemaRegistryConfig.props, true)
      Some(ks)
    } else None

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val (k, v) = element.toKV
    val key    =
      keySerializer.flatMap(ks => k.map(kk => ks.serialize(topic, kk)))
    val value  = valueSerializer.serialize(topic, v)
    new ProducerRecord(
      sinkConfig.expandTemplate(v),
      null,
      element.$timestamp,
      key.orNull,
      value
    )
  }

}
