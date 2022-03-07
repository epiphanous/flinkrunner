package io.epiphanous.flinkrunner.serde
import purecsv.safe._
import purecsv.safe.converter.RawFieldsConverter

/**
 * Write an element as a csv line
 * @param delimiter
 *   the delimiter, defaults to comma
 * @tparam E
 *   the element type
 */
class DelimitedLineEncoder[E](delimiter: String = ",")(implicit
    rfcImpl: RawFieldsConverter[E])
    extends TextLineEncoder[E] {
  override def encode(element: E): String =
    element.toCSV(delimiter) + System.lineSeparator()
}
