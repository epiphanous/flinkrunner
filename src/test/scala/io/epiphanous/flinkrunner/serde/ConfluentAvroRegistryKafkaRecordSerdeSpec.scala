package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation

class ConfluentAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("AWrapper Confluent Serde Roundtrip") {
    SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT](
      genOne[AWrapper]
    ).runTest
  }

  property("BWrapper Confluent Serde Roundtrip") {
    SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT](
      genOne[BWrapper]
    ).runTest
  }

}
