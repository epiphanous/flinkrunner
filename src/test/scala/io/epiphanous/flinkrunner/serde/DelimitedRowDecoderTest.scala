package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.SimpleB
import org.apache.flink.api.scala.createTypeInformation

class DelimitedRowDecoderTest extends PropSpec {

  property("decode property") {
    val decoder        = new DelimitedRowDecoder[SimpleB]()
    val simpleB        = genOne[SimpleB]
    val line           = simpleB.productIterator
      .map {
        case Some(x) => x.toString
        case None    => ""
        case x       => x.toString
      }
      .mkString(",") + System.lineSeparator()
    val decodedSimpleB = decoder.decode(line)
    decodedSimpleB.success.value.toString shouldEqual simpleB.toString
  }

}
