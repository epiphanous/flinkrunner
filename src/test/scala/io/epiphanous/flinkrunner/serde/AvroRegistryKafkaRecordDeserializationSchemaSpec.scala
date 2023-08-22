package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.Logger
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.slf4j.{Logger => UnderlyingLogger}

import java.util
import scala.util.Try

class AvroRegistryKafkaRecordDeserializationSchemaSpec
    extends PropSpec
    with MockitoSugar {

  val mockLogger: UnderlyingLogger = mock[UnderlyingLogger]
  when(mockLogger.isInfoEnabled).thenReturn(true)
  when(mockLogger.isErrorEnabled).thenReturn(true)

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
      : AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT] = {
    new AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      kafkaSourceConfig
    ) {
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
      override protected lazy val logger: Logger           = Logger(mockLogger)
    }
  }

  val emptyDeserializer: AvroRegistryKafkaRecordDeserializationSchema[
    BWrapper,
    BRecord,
    MyAvroADT
  ] = getDeserializer[BWrapper, BRecord, MyAvroADT]()

  def getConsumerRecord(key: Array[Byte] = null, value: Array[Byte] = null)
      : ConsumerRecord[Array[Byte], Array[Byte]] =
    new ConsumerRecord(topic, partition, offset, key, value)

  property("Tombstones are ignored") {
    val collected: util.List[BWrapper] = new util.ArrayList()
    val out                            = new ListCollector[BWrapper](collected)
    Try(
      emptyDeserializer.deserialize(getConsumerRecord(), out)
    ) should be a 'success
    collected should have length 0
    verify(mockLogger).info(
      s"ignoring null value kafka record ({})",
      recordInfo
    )
  }

  property("Deserialization errors log but don't fail") {
    val collected: util.List[BWrapper] = new util.ArrayList()
    val out                            = new ListCollector[BWrapper](collected)
    Try(
      emptyDeserializer.deserialize(
        getConsumerRecord(null, Array(101, 1, 2, 3)),
        out
      )
    ) should be a 'success
    collected should have length 0
    verify(mockLogger).error(
      ArgumentMatchers.anyString(),
      ArgumentMatchers.any(classOf[Exception])
    )
  }
}
