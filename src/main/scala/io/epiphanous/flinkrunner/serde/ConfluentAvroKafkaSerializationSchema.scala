package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  KafkaSinkConfig,
  KafkaSourceConfig
}
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

/**
 * A schema to serialize an ADT event using a confluent avro schema
 * registry. An implementing class must provide a Flink
 * [[ConfluentRegistryAvroSerializationSchema]] to interface with the
 * schema registry. That registry is specific to a type that implements
 * Avro's [[SpecificRecord]] interface type.
 * @param sinkName
 *   name of the sink stream
 * @param config
 *   flink runner config
 * @tparam E
 *   the event type we are serializing from, which is a member of the ADT
 * @tparam ADT
 *   the flink runner ADT
 */
abstract class ConfluentAvroKafkaSerializationSchema[
    E <: ADT,
    ADT <: FlinkEvent](
    sinkName: String,
    config: FlinkConfig[ADT]
) extends KafkaSerializationSchema[E]
    with LazyLogging {

  val sinkConfig: KafkaSinkConfig =
    config.getSourceConfig(sinkName).asInstanceOf[KafkaSinkConfig]

  override def serialize(
      element: E,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = ???
}
