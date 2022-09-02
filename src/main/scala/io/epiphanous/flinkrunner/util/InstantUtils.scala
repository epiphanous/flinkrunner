package io.epiphanous.flinkrunner.util

import java.time.Instant
import java.time.format.DateTimeFormatter

object InstantUtils {

  val dtf: DateTimeFormatter =
    DateTimeFormatter.ofPattern("/yyyy/MM/dd/HH")

  implicit class RichInstant(instant: Instant) {
    def prefixedTimePath(prefix: String): String =
      prefix + dtf.format(instant)
  }
}
