package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.parser._
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  SourceConfig
}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}

import java.nio.charset.StandardCharsets

/**
 * @param sourceName
 *   the name of the source we are deserializing from
 * @param config
 *   flink runner configuration
 * @tparam ADT
 *   the algebraic data type of our events
 */
class CirceJsonDeserializationSchema[ADT <: FlinkEvent](
    sourceName: String,
    config: FlinkConfig[ADT])(implicit
    circeDecoder: Decoder[ADT],
    ev: Null <:< ADT)
    extends DeserializationSchema[ADT]
    with LazyLogging {

  val sourceConfig: SourceConfig = config.getSourceConfig(sourceName)

  /**
   * Deserialize a json byte array into an ADT event instance or return
   * null if the byte array can't be successfully deserialized
   * @param bytes
   *   a json-encoded byte array
   * @return
   *   an instance of an ADT event type
   */
  override def deserialize(bytes: Array[Byte]): ADT = {
    val payload = new String(bytes, StandardCharsets.UTF_8)
    decode[ADT](payload).toOption match {
      case Some(event) => event
      case other       =>
        logger.error(
          s"Failed to deserialize JSON payload from source $sourceName: <start>$payload<end>"
        )
        other.orNull
    }
  }

  /**
   * Determine if the next event is the end of the stream or not. We always
   * return false since we assume the stream never ends.
   * @param nextEvent
   *   the next event
   * @return
   *   false
   */
  override def isEndOfStream(nextEvent: ADT): Boolean = false

  /**
   * Compute the produced type when deserializing a byte array
   * @return
   *   TypeInformation[E]
   */
  override def getProducedType: TypeInformation[ADT] =
    TypeInformation.of(new TypeHint[ADT] {})

}
