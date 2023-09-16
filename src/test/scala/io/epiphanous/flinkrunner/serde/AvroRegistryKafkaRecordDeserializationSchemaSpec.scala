package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.{MockLogger, PropSpec}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.mockito.ArgumentMatchers

import scala.util.Try

class AvroRegistryKafkaRecordDeserializationSchemaSpec extends PropSpec {

  val topic      = "topic"
  val partition  = 1
  val offset     = 1L
  val recordInfo = s"topic=$topic, partition=$partition, offset=$offset"

  def getDeserializer[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      kafkaSourceConfig: KafkaSourceConfig[ADT] = null,
      schemaRegistryClient: MockSchemaRegistryClient =
        new MockSchemaRegistryClient())(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT]
        with MockLogger = {
    new AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      kafkaSourceConfig
    ) with MockLogger {
      override val keyDeserializer: Deserializer[AnyRef]   =
        new StringDeserializerWithConfluentFallback(
          Some(
            new KafkaAvroDeserializer(
              schemaRegistryClient
            )
          )
        )
      override val valueDeserializer: Deserializer[AnyRef] =
        new KafkaAvroDeserializer(schemaRegistryClient)
    }
  }

  val emptyDeserializer = getDeserializer[BWrapper, BRecord, MyAvroADT]()

  def getConsumerRecord(key: Array[Byte] = null, value: Array[Byte] = null)
      : ConsumerRecord[Array[Byte], Array[Byte]] =
    new ConsumerRecord(topic, partition, offset, key, value)

  property("Tombstones are ignored") {
    val out = new SimpleListCollector[BWrapper]()
    Try(
      emptyDeserializer.deserialize(getConsumerRecord(), out)
    ) should be a 'success
    out should have length 0
    emptyDeserializer.didEmitLog(
      _.trace(s"ignoring null value kafka record ({})", recordInfo)
    )
  }

  property("Deserialization errors log but don't fail") {
    val out = new SimpleListCollector[BWrapper]()
    Try(
      emptyDeserializer.deserialize(
        getConsumerRecord(null, Array(101, 1, 2, 3)),
        out
      )
    ) should be a 'success
    out should have length 0
    emptyDeserializer.didEmitLog(
      _.error(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.any(classOf[Exception])
      )
    )
  }
}
