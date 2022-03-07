package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.syntax._

class JsonLineEncoder[E](
    pretty: Boolean = false,
    sortKeys: Boolean = false)(implicit circeEncoder: Encoder[E])
    extends TextLineEncoder[E]
    with LazyLogging {
  override def encode(event: E): String = {
    val e = event.asJson
    if (pretty) {
      if (sortKeys) e.spaces2SortKeys else e.spaces2
    } else {
      if (sortKeys) e.noSpacesSortKeys else e.noSpaces
    }
  }
}
