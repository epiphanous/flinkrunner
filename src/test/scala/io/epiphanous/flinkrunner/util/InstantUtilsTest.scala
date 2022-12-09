package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.util.InstantUtils.RichInstant

import java.time.Instant

class InstantUtilsTest extends PropSpec {

  property("Adds Prefix") {
    val prefix           = "prefix"
    val zero_time        = RichInstant(Instant.ofEpochSecond(0))
    val formatted_string = "prefix/1970/01/01/00"
    zero_time.prefixedTimePath(prefix) shouldEqual formatted_string
  }

  property("does not need a prefix") {
    val prefix           = ""
    val zero_time        = RichInstant(Instant.ofEpochSecond(0))
    val formatted_string = "/1970/01/01/00"
    zero_time.prefixedTimePath(prefix) shouldEqual formatted_string
  }

}
