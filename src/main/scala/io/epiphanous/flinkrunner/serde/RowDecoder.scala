package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.StreamFormatName
import io.epiphanous.flinkrunner.model.StreamFormatName._
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties
import scala.util.Try

abstract class RowDecoder[E: TypeInformation]
    extends Serializable
    with LazyLogging {
  def decode(line: String): Try[E]
}

object RowDecoder {
  def forEventType[E: TypeInformation](
      format: StreamFormatName,
      properties: Properties): RowDecoder[E] =
    format match {
      case Json => new JsonRowDecoder[E]
      case _    =>
        new DelimitedRowDecoder[E](DelimitedConfig.get(format, properties))
    }
}
