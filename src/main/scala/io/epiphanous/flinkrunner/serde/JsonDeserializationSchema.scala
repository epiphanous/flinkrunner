package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.SourceConfig
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.nio.charset.StandardCharsets

/**
 * Deserialize a json-encoded byte array to an event type.
 * @param sourceConfig
 *   the config for the source that is being deserialized
 * @tparam E
 *   the event type
 */
class JsonDeserializationSchema[E <: FlinkEvent: TypeInformation](
    sourceConfig: SourceConfig)
    extends DeserializationSchema[E]
    with LazyLogging {

  val typeInfo: TypeInformation[E] = implicitly[TypeInformation[E]]

  val sourceName: String = sourceConfig.name

  val jsonRowDecoder = new JsonRowDecoder[E]

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
    jsonRowDecoder
      .decode(payload)
      .fold(
        error =>
          throw new RuntimeException(
            s"failed to deserialize event: ${error.getMessage}"
          ),
        e => e
      )
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
    typeInfo

}
