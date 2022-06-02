package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.StreamFormatName
import io.epiphanous.flinkrunner.model.StreamFormatName._
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties
import scala.util.Try

abstract class RowEncoder[E: TypeInformation]
    extends Serializable
    with LazyLogging {
  def encode(element: E): Try[String]
}

object RowEncoder {
  def forEventType[E: TypeInformation](
      format: StreamFormatName,
      properties: Properties): RowEncoder[E] = {
    format match {
      case Json =>
        new JsonRowEncoder(
          properties.getProperty("pretty", "false").toBoolean,
          properties.getProperty("sort.keys", "false").toBoolean
        )
      case _    =>
        new DelimitedRowEncoder(DelimitedConfig.get(format, properties))
    }
  }
}
