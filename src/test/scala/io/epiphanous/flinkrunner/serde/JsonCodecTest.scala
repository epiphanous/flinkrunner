package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BRecord, CRecord}

import java.nio.charset.StandardCharsets
import scala.util.Try

class JsonCodecTest extends PropSpec with JsonCodec {

  property("getMapper property") {
    val record = genOne[CRecord]
    logger.debug(record.toString)
    val output =
      Try(getMapper(classOf[CRecord]).writer().writeValueAsString(record))
    println(output)
    output isSuccess
  }

}
