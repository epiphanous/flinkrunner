package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  KafkaSourceConfig
}
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A schema to deserialize bytes from kafka into an ADT event using a
 * confluent avro schema registry. An implementing class must provide a
 * Flink [[ConfluentRegistryAvroDeserializationSchema]] to interface with
 * the schema registry. That registry is specific to a type that implements
 * Avro's [[SpecificRecord]] interface. The implementing class must also
 * provide a [[deserializeSpecificRecord]] method that deserializes an
 * array of bytes into a specific record type as well as a
 * [[fromSpecificRecord]] method that converts that type into a type that
 * is a member of the ADT.
 *
 * @param sourceName
 *   name of the source stream
 * @param config
 *   flink runner config
 * @tparam E
 *   the event type we are producing here, which is a member of the ADT
 * @tparam ADT
 *   the flink runner ADT
 */
abstract class ConfluentAvroRegistryKafkaDeserializationSchema[
    E <: ADT,
    ADT <: FlinkEvent
](
    sourceName: String,
    config: FlinkConfig[ADT]
) extends KafkaDeserializationSchema[E]
    with LazyLogging {

  val sourceConfig: KafkaSourceConfig =
    config.getSourceConfig(sourceName).asInstanceOf[KafkaSourceConfig]

  /**
   * Implementing classes must provide a a confluent schema registry
   * deserialization schema for specific records of type T.
   * @tparam K
   *   specific record type
   * @return
   *   ConfluentRegistryAvroDeserializationSchema[K]
   */
  def schemaRegistryKeyDeserializer[K]
      : ConfluentRegistryAvroDeserializationSchema[K]

  /**
   * A helper method to use the provided schema registry deserialization
   * schema to deserialize a kafka message into a specific record instance.
   * @param message
   *   the kafka message
   * @return
   *   an instance of specific record type T
   */
  def deserializeSpecificRecord[T <: SpecificRecord](
      message: Array[Byte],
      isKey: Boolean = false): T = ???
  ///schemaRegistryDeserializer.deserialize(message)

  /**
   * Convert a deserialized specific record instance into an instance of
   * our produced event type. Must be defined by implementing classes.
   * @param key
   *   an optional key of type K
   * @param value
   *   a value of specific record type V
   * @tparam K
   *   the type of the key
   * @tparam V
   *   the type of the value, subtype of avro specific record
   * @return
   *   an instance of the flink runner ADT
   */
  def fromSpecificRecord[K, V <: SpecificRecord](
      key: Option[K],
      value: V): E

  def deserializeKey[K](key: Array[Byte]): K

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]]): E = ???
//  {
//    val key =
//      if (sourceConfig.isKeyed) Some(deserializeKey(record.key()))
//      else None
//    fromSpecificRecord(
//      key,
//      schemaRegistryDeserializer.deserialize(record.value())
//    )
//  }

  override def isEndOfStream(nextElement: E): Boolean = false

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
