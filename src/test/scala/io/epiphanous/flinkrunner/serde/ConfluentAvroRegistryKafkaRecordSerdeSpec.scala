package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.scala._
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util
import scala.util.Try
//import org.apache.flink.api.scala._ //<--- optimize imports might remove this

class ConfluentAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("Ignore tombstone records") {
    val test      = SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT]()
    val list      = new util.ArrayList[AWrapper]()
    val collector = new ListCollector[AWrapper](list)
    val tombstone = new ConsumerRecord[Array[Byte], Array[Byte]](
      "test-topic",
      1,
      1L,
      null,
      null
    )
    Try(
      test.getConfluentDeserializer.deserialize(tombstone, collector)
    ) should be a 'success
    list should have size 0
  }

  property("AWrapper confluent serde round trip") {
    val test = SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT]()
    test.init()
    test.runTest(genOne[AWrapper])
  }

  property("BWrapper confluent serde round trip scale") {
    val test = SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT]()
    test.init()
    genPop[BWrapper](1000).zipWithIndex.foreach { case (b, i) =>
      if (i % 100 == 0) println(i)
      test.runTest(b)
    }
  }

}
