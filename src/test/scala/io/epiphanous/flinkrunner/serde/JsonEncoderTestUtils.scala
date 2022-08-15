package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.BasePropGenerators
import org.scalacheck.{Arbitrary, Gen}
import org.apache.flink.api.scala.createTypeInformation

trait JsonEncoderTestUtils extends BasePropGenerators {
  case class JsonEncoderTest(a: Int, b: String) {
    def serialize: String = s"""{"a":$a,"b":"$b"}"""
  }

  val genTest: Gen[JsonEncoderTest]                = for {
    a <- Gen.chooseNum[Int](1, 100)
    b <- nameGen("test")
  } yield JsonEncoderTest(a, b)
  implicit val arbTest: Arbitrary[JsonEncoderTest] = Arbitrary(
    genTest
  )

  def getFileEncoder(pretty: Boolean = false, sortKeys: Boolean = false) =
    new JsonFileEncoder[JsonEncoderTest](pretty, sortKeys)

  def getRowEncoder(pretty: Boolean = false, sortKeys: Boolean = false) =
    new JsonRowEncoder[JsonEncoderTest](pretty, sortKeys)
}
