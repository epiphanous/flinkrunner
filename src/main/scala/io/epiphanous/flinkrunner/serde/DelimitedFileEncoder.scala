package io.epiphanous.flinkrunner.serde
import org.apache.flink.api.common.serialization.Encoder
import purecsv.safe._
import purecsv.safe.converter.RawFieldsConverter

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/**
 * Write an element as CSV
 * @param delimiter
 *   the delimiter, defaults to comma
 * @tparam E
 *   the element type
 */
class DelimitedFileEncoder[E](delimiter: String = ",")(implicit
    rfcImpl: RawFieldsConverter[E])
    extends Encoder[E] {
  override def encode(element: E, stream: OutputStream): Unit = {
    stream.write(
      (element.toCSV(delimiter) + System.lineSeparator())
        .getBytes(StandardCharsets.UTF_8)
    )
  }
}
