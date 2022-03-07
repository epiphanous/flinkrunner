package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import purecsv.safe._
import purecsv.safe.converter.RawFieldsConverter

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

class DelimitedLineDecoder[E: ClassTag](delimiter: String = ",")(implicit
    rfcImpl: RawFieldsConverter[E],
    ev: Null <:< E)
    extends TextLineDecoder[E]
    with LazyLogging {
  override def decode(line: String): E = {
    CSVReader[E].readCSVFromString(line, delimiter.head).head match {
      case Success(event)     => event
      case Failure(exception) =>
        logger.error(
          s"DelimitedLineDecoder::decode failed on $line\n${exception.getMessage}"
        )
        None.orNull
    }
  }
}
