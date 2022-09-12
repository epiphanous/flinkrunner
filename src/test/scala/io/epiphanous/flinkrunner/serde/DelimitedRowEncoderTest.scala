package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BRecord, CRecord, SimpleB}
import org.apache.flink.api.scala.createTypeInformation

class DelimitedRowEncoderTest
    extends PropSpec
    with DelimitedEncoderTestUtils {

  property("encode property") {
    val encoder = getRowEncoder()
    forAll { test: DelimitedEncoderTest =>
      encoder
        .encode(test)
        .fold(
          t => fail(t.getMessage),
          _ shouldEqual test.serialize
        )
    }
  }

  property("encode simpleB") {
    val encoder = getTypedRowEncoder[SimpleB]()
    forAll { b: SimpleB =>
      val encoded = encoder.encode(b)
      encoded should be a 'success
    }
  }

  property("encode non-nested avro property") {
    def csvLine(b: BRecord): String =
      s"${b.b0},${b.b1.getOrElse("")},${b.b2.getOrElse("")},${b.b3.toEpochMilli}${System.lineSeparator()}"
    val encoder                     = getTypedRowEncoder[BRecord]()
    forAll { test: BRecord =>
      encoder
        .encode(test)
        .fold(t => fail(t.getMessage), _ shouldEqual csvLine(test))
    }
  }

  property("fail to encode nested avro property") {
    val encoder = new DelimitedRowEncoder[CRecord]()
    forAll { test: CRecord =>
      val encoded = encoder.encode(test)
      if (test.bRecord.nonEmpty)
        encoded should be a 'failure
      else encoded should be a 'success
    }
  }

}
