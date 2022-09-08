package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.scalacheck.Arbitrary

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

class EmbeddedAvroJsonFileEncoderTest extends PropSpec {

  def doTest[
      E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      pretty: Boolean = false,
      sortKeys: Boolean = false)(implicit arb: Arbitrary[E]) = {
    val pop      = genPop[E]()
    val encoder  =
      new EmbeddedAvroJsonFileEncoder[E, A, MyAvroADT](pretty, sortKeys)
    val baos     = new ByteArrayOutputStream()
    val lines    = ArrayBuffer.empty[String]
    pop.foreach { w =>
      encoder.encode(w, baos)
      lines += w.toJson(pretty, sortKeys)
    }
    lines += ""
    val actual   = new String(baos.toByteArray, StandardCharsets.UTF_8)
    val expected = lines.mkString(System.lineSeparator())
    actual shouldEqual expected
//    println("expected:\n" + expected + "----")
//    println("actual:\n" + actual + "----")
  }

  property("encode awrapper property") {
    doTest[AWrapper, ARecord]()
  }

  property("encode bwrapper property") {
    doTest[BWrapper, BRecord]()
  }

  property("encode cwrapper property") {
    doTest[CWrapper, CRecord]()
  }

}
