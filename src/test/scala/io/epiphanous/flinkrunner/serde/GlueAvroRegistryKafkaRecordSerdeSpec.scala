package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation

class GlueAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("AWrapper Glue Serde Roundtrip") {
    implicit val ati = createTypeInformation[AWrapper]
    SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT](
      genOne[AWrapper],
      defaultGlueSchemaRegConfigStr
    ).runTest
  }

  property("BWrapper Glue Serde Roundtrip") {
    SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT](
      genOne[BWrapper]
    ).runTest
  }

}
