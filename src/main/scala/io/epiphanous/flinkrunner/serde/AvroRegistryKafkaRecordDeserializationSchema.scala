package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.KafkaInfoHeader._
import io.epiphanous.flinkrunner.model.SchemaRegistryType.{
  AwsGlue,
  Confluent
}
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.Try

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
  * @tparam E
  *   event type being deserialized, with an embedded avro record
  * @tparam A
  *   avro record type embedded within E
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
abstract class AvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sourceConfig: KafkaSourceConfig[ADT]
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val avroClass: Class[A]   = implicitly[TypeInformation[A]].getTypeClass
  val avroClassName: String = avroClass.getCanonicalName

  val keyDeserializer: Deserializer[AnyRef]
  val valueDeserializer: Deserializer[AnyRef]

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {

    val headers = Option(record.headers())
      .map(_.asScala.map { h =>
        (h.key(), new String(h.value(), StandardCharsets.UTF_8))
      }.toMap)
      .getOrElse(Map.empty[String, String]) ++ Map(
      headerName(SerializedValueSize) -> record
        .value()
        .length
        .toString,
      headerName(SerializedKeySize)   -> record.key().length.toString,
      headerName(Offset)              -> record.offset().toString,
      headerName(Partition)           -> record.partition().toString,
      headerName(Timestamp)           -> record.timestamp().toString,
      headerName(TimestampType)       -> record
        .timestampType()
        .name(),
      headerName(Topic)               -> record.topic()
    )

    // deserialize the key
    val keyDeserialized = Try(
      keyDeserializer.deserialize(record.topic(), record.key())
    ).map(_.toString)

    val keyOpt = keyDeserialized
      .fold(
        error => {
          logger.error(
            s"failed to deserialize kafka message key (${record
                .key()
                .length} bytes) from topic ${record.topic()}",
            error
          )
          None
        },
        k => Some(k)
      )

    val valueDeserialized = Try(
      valueDeserializer.deserialize(record.topic(), record.value())
    )

    valueDeserialized
      .fold(
        error =>
          logger.error(
            s"failed to deserialize kafka message value (${record
                .value()
                .length} bytes) to $avroClassName",
            error
          ),
        {
          case rec: A @unchecked =>
            val event = fromKV(
              EmbeddedAvroRecordInfo(
                rec,
                sourceConfig.config,
                keyOpt,
                headers
              )
            )
            out.collect(event)
          case unexpected        =>
            logger.error(
              s"Expected deserialized kafka message value of type $avroClassName, but got [$unexpected]"
            )
        }
      )
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}

object AvroRegistryKafkaRecordDeserializationSchema {

  def apply[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent](sourceConfig: KafkaSourceConfig[ADT])(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT] = {

    sourceConfig.schemaRegistryConfig.schemaRegistryType match {
      case Confluent =>
        new ConfluentAvroRegistryKafkaRecordDeserializationSchema[
          E,
          A,
          ADT
        ](sourceConfig)
      case AwsGlue   =>
        new GlueAvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
          sourceConfig
        )
    }
  }
}
