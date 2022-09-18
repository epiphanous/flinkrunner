package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

abstract class RowDecoder[E: TypeInformation]
    extends Serializable
    with LazyLogging {
  def decode(line: String): Try[E]
}
