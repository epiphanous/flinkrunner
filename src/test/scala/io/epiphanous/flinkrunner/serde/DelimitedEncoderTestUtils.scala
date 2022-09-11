package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.BasePropGenerators
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.scalacheck.{Arbitrary, Gen}

trait DelimitedEncoderTestUtils extends BasePropGenerators {
  case class DelimitedEncoderTest(a: Int, b: String, c: Option[Double]) {
    def serialize: String = s"$a,$b,${c.getOrElse("")}\n"
  }

  val genTest: Gen[DelimitedEncoderTest]                = for {
    a <- Gen.chooseNum[Int](1, 100)
    b <- nameGen("test")
    c <- Gen.option(Gen.chooseNum(100d, 900d))
  } yield DelimitedEncoderTest(a, b, c)
  implicit val arbTest: Arbitrary[DelimitedEncoderTest] = Arbitrary(
    genTest
  )

  /** TODO: improve support for testing different delimited configs
    */
  def getFileEncoder(
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV) =
    new DelimitedFileEncoder[DelimitedEncoderTest](delimitedConfig)

  def getRowEncoder(
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV) =
    new DelimitedRowEncoder[DelimitedEncoderTest](delimitedConfig)

  def getTypedRowEncoder[E: TypeInformation](
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV) =
    new DelimitedRowEncoder[E](delimitedConfig)
}
