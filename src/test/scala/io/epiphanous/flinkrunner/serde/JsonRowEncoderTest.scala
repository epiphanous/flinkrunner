package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec

class JsonRowEncoderTest extends PropSpec with JsonEncoderTestUtils {

  property("encode property") {
    val encoder = getRowEncoder()
    forAll { test: JsonEncoderTest =>
      encoder.encode(test).foreach(_ shouldEqual (test.serialize + "\n"))
    }
  }
}
