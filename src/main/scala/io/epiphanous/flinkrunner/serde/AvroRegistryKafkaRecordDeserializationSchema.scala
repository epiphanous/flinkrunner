package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
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
import io.epiphanous.flinkrunner.util.AvroUtils.{
  isSpecific,
  toEmbeddedAvroInstance
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

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

  val avroClass: Class[A]          = implicitly[TypeInformation[A]].getTypeClass
  val avroClassName: String        = avroClass.getCanonicalName
  val avroClassIsSpecific: Boolean = isSpecific(avroClass)

  val keyDeserializer: Deserializer[AnyRef]
  val valueDeserializer: Deserializer[AnyRef]

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {

    val recordInfo = s"topic=${record.topic()}, partition=${record
        .partition()}, offset = ${record.offset()}"

    // shortcut process if record value is tombstone
    if (Option(record.value()).isEmpty) {
      logger.info(s"ignoring null value kafka record ($recordInfo)")
      return
    }

    val recordValueLength =
      Option(record.value()).map(_.length).getOrElse(-1)
    val recordKeyLength   = Option(record.key()).map(_.length).getOrElse(-1)

    val headers = Option(record.headers())
      .map(
        _.asScala
          .filterNot(_.value() == null)
          .map { h =>
            (h.key(), new String(h.value(), StandardCharsets.UTF_8))
          }
          .toMap
      )
      .getOrElse(Map.empty[String, String]) ++ Map(
      headerName(SerializedValueSize) -> recordValueLength.toString,
      headerName(SerializedKeySize)   -> recordKeyLength.toString,
      headerName(Offset)              -> record.offset().toString,
      headerName(Partition)           -> record.partition().toString,
      headerName(Timestamp)           -> Option(record.timestamp())
        .map(_.toString)
        .orNull,
      headerName(TimestampType)       -> record
        .timestampType()
        .name(),
      headerName(Topic)               -> record.topic()
    )

    (for {
      keyDeserialized <-
        Try(
          keyDeserializer.deserialize(record.topic(), record.key())
        )
      keyOpt <-
        Success(Option(keyDeserialized).map(_.toString))
      valueDeserialized <-
        Try(
          valueDeserializer.deserialize(record.topic(), record.value())
        )
      record <- valueDeserialized match {
                  case rec: GenericRecord => Success(rec)
                  case other              =>
                    Failure(
                      new RuntimeException(
                        s"Expected deserialized kafka message value of type $avroClassName, but got [$other]"
                      )
                    )
                }
      event <- toEmbeddedAvroInstance[E, A, ADT](
                 record,
                 avroClass,
                 sourceConfig.config,
                 keyOpt,
                 headers
               )
      ok <- Try(out.collect(event))
    } yield ok).fold(
      error =>
        logger.error(
          s"Logging failure to deserialize kafka message ($recordInfo), continuing processing stream",
          error
        ),
      _ => ()
    )

  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}

object AvroRegistryKafkaRecordDeserializationSchema {

  def apply[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent](
      sourceConfig: KafkaSourceConfig[ADT],
      schemaRegistryClientOpt: Option[SchemaRegistryClient] = None)(
      implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
      : AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT] = {

    sourceConfig.schemaRegistryConfig.schemaRegistryType match {
      case Confluent =>
        new ConfluentAvroRegistryKafkaRecordDeserializationSchema[
          E,
          A,
          ADT
        ](sourceConfig, schemaRegistryClientOpt)
      case AwsGlue   =>
        new GlueAvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
          sourceConfig,
          schemaRegistryClientOpt
        )
    }
  }
}
