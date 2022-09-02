package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  MyAvroADT,
  StreamFormatName
}
import org.apache.flink.api.scala.createTypeInformation

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Properties
import collection.JavaConverters._

class EmbeddedAvroDelimitedFileEncoderTest extends PropSpec {

  val delimitedConfig = DelimitedConfig.get(
    StreamFormatName.Psv,
    new Properties(),
    ARecord.SCHEMA$.getFields.asScala.map(_.name()).toList
  )

  val encoder =
    new EmbeddedAvroDelimitedFileEncoder[AWrapper, ARecord, MyAvroADT](
      delimitedConfig
    )

  def out2String(baos: ByteArrayOutputStream) =
    new String(baos.toByteArray, StandardCharsets.UTF_8)

  property("encode property") {
    val baos   = new ByteArrayOutputStream()
    forAll { a: AWrapper =>
      encoder.encode(a, baos)
    }
    val result = out2String(baos).split(delimitedConfig.lineSeparator)
    result.length shouldBe 11
    result.head shouldEqual "a0|a1|a2|a3"
  }

  property("encode different output stream property") {
    val baos1   = new ByteArrayOutputStream()
    forAll { a: AWrapper =>
      encoder.encode(a, baos1)
    }
    val result1 = out2String(baos1).split(delimitedConfig.lineSeparator)

    val baos2   = new ByteArrayOutputStream()
    forAll { a: AWrapper =>
      encoder.encode(a, baos2)
    }
    val result2 = out2String(baos2).split(delimitedConfig.lineSeparator)
    result1.length shouldEqual 11
    result1.length shouldEqual result2.length
    result1.head shouldEqual result2.head
  }

}
