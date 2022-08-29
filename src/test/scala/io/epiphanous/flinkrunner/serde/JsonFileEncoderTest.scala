package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import org.apache.commons.io.output.ByteArrayOutputStream

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

class JsonFileEncoderTest extends PropSpec with JsonEncoderTestUtils {

  property("encode property") {
    val encoder  = getFileEncoder()
    val baos     = new ByteArrayOutputStream()
    val lines    = ListBuffer.empty[String]
    forAll { test: JsonEncoderTest =>
      encoder.encode(test, baos)
      lines += test.serialize
    }
    val actual   = new String(baos.toByteArray, StandardCharsets.UTF_8)
    val expected = lines.mkString("", "\n", "\n")
//    logger.debug("actual:\n" + actual)
//    logger.debug("expected:\n" + expected)
    actual shouldEqual expected
  }
}
