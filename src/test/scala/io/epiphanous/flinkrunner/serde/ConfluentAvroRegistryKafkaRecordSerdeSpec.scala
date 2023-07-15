package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation

class ConfluentAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("AWrapper Confluent Serde Roundtrip") {
    SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT](
      genOne[AWrapper]
    ).runTest
  }

  property("BWrapper scale test") {
    genPop[BWrapper](10000).zipWithIndex.foreach { case (b, i) =>
      if (i % 100 == 0) println(i)
      SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT](b).runTest
    }
  }

}
