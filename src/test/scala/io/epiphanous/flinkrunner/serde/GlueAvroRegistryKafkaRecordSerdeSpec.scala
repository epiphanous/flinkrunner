package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala._

class GlueAvroRegistryKafkaRecordSerdeSpec extends SerdeTestFixtures {

  property("AWrapper Glue Serde Roundtrip") {
    SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT](
      genOne[AWrapper],
      defaultGlueSchemaRegConfigStr
    ).runTest
  }

  property("BWrapper Glue Serde Roundtrip Scale") {
    genPop[BWrapper](10000).zipWithIndex.foreach { case (b, i) =>
      if (i % 100 == 0) println(i)
      SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT](
        b,
        defaultGlueSchemaRegConfigStr
      ).runTest
    }

  }

}
