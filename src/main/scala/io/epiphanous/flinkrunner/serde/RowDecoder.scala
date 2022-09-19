package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.Try

abstract class RowDecoder[E: TypeInformation]
    extends Serializable
    with LazyLogging {
  def decode(line: String): Option[E] = {
    tryDecode(line)
      .fold(
        error => {
          logger.error(s"failed to decode line: ${line.trim}", error)
          None // ugggh
        },
        event => Some(event)
      )
  }

  def tryDecode(line: String): Try[E]
}
