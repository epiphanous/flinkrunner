package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.BasePropGenerators
import org.apache.flink.api.scala.createTypeInformation
import org.scalacheck.{Arbitrary, Gen}

trait JsonEncoderTestUtils extends BasePropGenerators {
  case class JsonEncoderTest(a: Int, b: String, c: Option[Double]) {
    def serialize: String = s"""{"a":$a,"b":"$b","c":${c.orNull}}"""
  }

  val genTest: Gen[JsonEncoderTest]                = for {
    a <- Gen.chooseNum[Int](1, 100)
    b <- nameGen("test")
    c <- Gen.option(Gen.chooseNum(100d, 900d))
  } yield JsonEncoderTest(a, b, c)
  implicit val arbTest: Arbitrary[JsonEncoderTest] = Arbitrary(
    genTest
  )

  /** TODO: improve support for testing pretty/sortkeys
    */
  def getFileEncoder(jsonConfig: JsonConfig = JsonConfig()) =
    new JsonFileEncoder[JsonEncoderTest](jsonConfig)

  def getRowEncoder(jsonConfig: JsonConfig = JsonConfig()) =
    new JsonRowEncoder[JsonEncoderTest](jsonConfig)
}
