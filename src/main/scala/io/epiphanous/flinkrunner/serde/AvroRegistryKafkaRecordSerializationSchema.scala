package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.SchemaRegistryType.{
  AwsGlue,
  Confluent
}
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import io.epiphanous.flinkrunner.util.SinkDestinationNameUtils.RichSinkDestinationName
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serializer

import java.lang
import java.nio.charset.StandardCharsets

/** A serialization schema that uses a schema registry to serialize a kafka
  * key/value pair into instances of a flink runner ADT that also
  * implements the EmbeddedAvroRecord trait.
  * @param sinkConfig
  *   config for the kafka source
  * @param schemaRegistryClientOpt
  *   optional schema registry client (useful for testing)
  * @tparam E
  *   event type being deserialized, with an embedded avro record
  * @tparam A
  *   avro record type embedded within E
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
abstract class AvroRegistryKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sinkConfig: KafkaSinkConfig[ADT]
) extends KafkaRecordSerializationSchema[E]
    with LazyLogging {

  val avroClass: Class[A]   = implicitly[TypeInformation[A]].getTypeClass
  val avroClassName: String = avroClass.getCanonicalName

  val keySerializer: Serializer[AnyRef]
  val valueSerializer: Serializer[AnyRef]

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

    val key = info.keyOpt.map { k =>
      keySerializer.serialize(
        topic,
        k
      )
    }

    val value = valueSerializer.serialize(topic, info.record)

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

object AvroRegistryKafkaRecordSerializationSchema {

  def apply[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent](sinkConfig: KafkaSinkConfig[ADT])
      : AvroRegistryKafkaRecordSerializationSchema[E, A, ADT] = {

    sinkConfig.schemaRegistryConfig.schemaRegistryType match {
      case Confluent =>
        new ConfluentAvroRegistryKafkaRecordSerializationSchema[
          E,
          A,
          ADT
        ](sinkConfig)
      case AwsGlue   =>
        new GlueAvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
          sinkConfig
        )
    }
  }
}
