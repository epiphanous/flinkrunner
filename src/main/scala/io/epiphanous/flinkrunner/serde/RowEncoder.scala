package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

abstract class RowEncoder[E: TypeInformation]
    extends Serializable
    with LazyLogging {
  def encode(element: E): Try[String]
}
