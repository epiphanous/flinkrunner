package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec

class JsonRowEncoderTest extends PropSpec with JsonEncoderTestUtils {

  property("encode property") {
    val encoder = getRowEncoder()
    forAll { test: JsonEncoderTest =>
      encoder
        .encode(test)
        .fold(
          t => fail(t.getMessage),
          _ shouldEqual (test.serialize + System.lineSeparator())
        )
    }
  }
}
