package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala._

class ConfluentAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("AWrapper confluent serde round trip") {
    val test = SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT]()
    test.init()
    test.runTest(genOne[AWrapper])
  }

  property("BWrapper confluent serde round trip scale") {
    val test = SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT]()
    test.init()
    genPop[BWrapper](10000).zipWithIndex.foreach { case (b, i) =>
      if (i % 100 == 0) println(i)
      test.runTest(b)
    }
  }

}
