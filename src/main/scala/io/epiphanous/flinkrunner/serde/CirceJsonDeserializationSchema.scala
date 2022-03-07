package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.parser._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}

import java.nio.charset.StandardCharsets

/**
 * Deserialize a json-encoded byte array to an event type. This requires an
 * implicit circe decoder instance for the type to be available.
 * @param sourceName
 *   the name of the source that is being deserialized
 * @tparam E
 *   the event type
 */
class CirceJsonDeserializationSchema[E](sourceName: String = "unknown")(
    implicit
    circeDecoder: Decoder[E],
    ev: Null <:< E)
    extends DeserializationSchema[E]
    with LazyLogging {

  /**
   * Deserialize a json byte array into an event instance or return null if
   * the byte array can't be successfully deserialized
   * @param bytes
   *   a json-encoded byte array
   * @return
   *   an instance of an event type
   */
  override def deserialize(bytes: Array[Byte]): E = {
    val payload = new String(bytes, StandardCharsets.UTF_8)
    decode[E](payload).toOption match {
      case Some(event) => event
      case other       =>
        logger.error(
          s"Failed to deserialize JSON payload from source $sourceName: $payload"
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
  override def isEndOfStream(nextEvent: E): Boolean = false

  /**
   * Compute the produced type when deserializing a byte array
   * @return
   *   [[TypeInformation]] [E]
   */
  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})

}
