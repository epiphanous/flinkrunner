package io.epiphanous.flinkrunner.serde
import io.circe.Encoder
import io.circe.syntax._
import org.apache.flink.api.common.serialization.{Encoder => FlinkEncoder}

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/**
 * Encoder for writing an element to a binary file output stream
 * @tparam E
 *   the element type
 */
class JsonFileEncoder[E](implicit circeEncoder: Encoder[E])
    extends FlinkEncoder[E] {
  override def encode(element: E, stream: OutputStream): Unit =
    stream.write(
      (element.asJson.noSpaces + System.lineSeparator())
        .getBytes(StandardCharsets.UTF_8)
    )
}
