package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  BRecord,
  BWrapper
}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.mutable

class ConfluentAvroRegistryKafkaRecordDeserializationSchemaTest
    extends SerdeTestFixtures {

  // ignore until set up testcontainers schema registry
  ignore("deserialize works for bwrapper") {
    val serde     = getDeserializerFor[BWrapper, BRecord]
    val collected = mutable.ArrayBuffer.empty[BWrapper]
    val collector = new Collector[BWrapper] {
      override def collect(t: BWrapper): Unit = collected += t
      override def close(): Unit              = {}
    }
//    showBytes("bWrapper key  ", bConsumerRecord.key())
//    showBytes("bWrapper value", bConsumerRecord.value())
    serde.deserialize(bConsumerRecord, collector)
    collected.head shouldBe an[BWrapper]
    collected.head shouldEqual bWrapper
  }

  ignore("deserialize works for awrapper") {
    val serde     = getDeserializerFor[AWrapper, ARecord]
    val collected = mutable.ArrayBuffer.empty[AWrapper]
    val collector = new Collector[AWrapper] {
      override def collect(t: AWrapper): Unit = collected += t
      override def close(): Unit              = {}
    }
//    showBytes("aWrapper key  ", aConsumerRecord.key())
//    showBytes("aWrapper value", aConsumerRecord.value())
    serde.deserialize(aConsumerRecord, collector)
    collected.head shouldBe an[AWrapper]
    collected.head shouldEqual aWrapper
  }

}
