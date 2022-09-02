package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

class JsonKafkaRecordDeserializationSchema[
    E <: ADT: TypeInformation,
    ADT <: FlinkEvent](kafkaSourceConfig: KafkaSourceConfig[ADT])
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val deserializationSchema =
    new JsonDeserializationSchema[E, ADT](kafkaSourceConfig)

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val element = deserializationSchema.deserialize(record.value())
    if (element != null) out.collect(element)
  }

  override def getProducedType: TypeInformation[E] =
    deserializationSchema.getProducedType
}
