package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  KafkaSourceConfig
}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util

/**
 * A deserialization schema that uses the provided confluent avro schema
 * registry client and `fromKV` partial function to deserialize a kafka
 * key/value pair into an instance of a flink runner ADT.
 *
 * In order to decouple the shape of the flink runner ADT types from the
 * types that are serialized in kafka, a user of this class must provide a
 * `fromKV` partial function that maps from the specific key and value pair
 * (key is optional) deserialized from the kafka source into an instance of
 * the flink runner ADT.
 *
 * Usually, `fromKV` is as simple as providing a set of cases. Consider the
 * following example, where `A` and `B` are subclasses of the flink runner
 * ADT, and `ASpecific` and `BSpecific` are corresponding Avro
 * `SpecificRecord` classes generated from avro schemas. In this case, we
 * ignore the key and have defined our ADT types to be wrappers around the
 * deserialized records. However, you can use the deserialized key and
 * value in any way that makes sense for your application.
 * {{{
 *   {
 *     //    (key,value)     => ADT
 *     case (_, a:ASpecific) => A(a)
 *     case (_, b:BSpecific) => B(b)
 *   }
 * }}}
 * @param sourceName
 *   name of the kafka source
 * @param config
 *   flink runner config
 * @param schemaRegistryClient
 *   the schema registry client
 * @param fromKV
 *   a partial function that should return a flink runner adt instance when
 *   passed a deserialized kafka key/value pair
 * @tparam ADT
 *   the flink runner ADT type
 */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    ADT <: FlinkEvent
](
    sourceName: String,
    config: FlinkConfig[ADT],
    schemaRegistryClient: SchemaRegistryClient,
    fromKV: PartialFunction[(Option[AnyRef], AnyRef), ADT]
) extends KafkaRecordDeserializationSchema[ADT]
    with LazyLogging {

  val sourceConfig: KafkaSourceConfig = {
    val sc = config.getSourceConfig(sourceName)
    if (sc.connector != FlinkConnectorName.Kafka)
      throw new RuntimeException(
        s"Requested source $sourceName is not a kafka source"
      )
    sc.asInstanceOf[KafkaSourceConfig]
  }

  val schemaRegistryProps: util.HashMap[String, String] =
    sourceConfig.propertiesMap

  val topic: String = sourceConfig.topic

  val valueDeserializer = new KafkaAvroDeserializer(
    schemaRegistryClient,
    schemaRegistryProps
  )

  val keyDeserializer: Option[KafkaAvroDeserializer] =
    if (sourceConfig.isKeyed) {
      val ks = new KafkaAvroDeserializer(schemaRegistryClient)
      ks.configure(schemaRegistryProps, true)
      Some(ks)
    } else None

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[ADT]): Unit = {
    val key   =
      keyDeserializer.map(ds => ds.deserialize(topic, record.key()))
    val value = valueDeserializer.deserialize(topic, record.value())
    if (Option(value).nonEmpty) out.collect(fromKV(key, value))
  }

  override def getProducedType: TypeInformation[ADT] =
    TypeInformation.of(new TypeHint[ADT] {})
}
