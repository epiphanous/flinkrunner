package io.epiphanous.flinkrunner.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

object InstantUtils {

  val dtf: DateTimeFormatter =
    DateTimeFormatter.ofPattern("/yyyy/MM/dd/HH").withZone(ZoneOffset.UTC)

  implicit class RichInstant(instant: Instant) {
    def prefixedTimePath(prefix: String): String =
      prefix + dtf.format(instant)
  }
}
