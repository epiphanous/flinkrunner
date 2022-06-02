package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.sink.SinkConfig
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.nio.charset.StandardCharsets

/**
 * Serialize an event instance into a json-encoded byte array. This
 * requires an implicit circe encoder instance for the type to be
 * available.
 * @param sinkName
 *   name of the sink we're serializing to
 * @param pretty
 *   if true, serialize with indentation and spacing
 * @param sortKeys
 *   if true, serialize with sorted keys
 * @tparam E
 *   the event type
 */
class JsonSerializationSchema[E: TypeInformation](sinkConfig: SinkConfig)
    extends SerializationSchema[E]
    with LazyLogging {

  val name: String      = sinkConfig.name
  val pretty: Boolean   =
    sinkConfig.properties.getProperty("pretty", "false").toBoolean
  val sortKeys: Boolean =
    sinkConfig.properties.getProperty("sort.keys", "false").toBoolean

  val jsonRowEncoder = new JsonRowEncoder[E](pretty, sortKeys)

  /**
   * Serialize an event into json-encoded byte array
   * @param event
   *   an event instance
   * @return
   *   a json encoded byte array
   */
  override def serialize(event: E): Array[Byte] = {
    jsonRowEncoder
      .encode(event)
      .fold(
        error =>
          throw new RuntimeException(
            s"failed to serialize event $event: ${error.getMessage}"
          ),
        _.getBytes(StandardCharsets.UTF_8)
      )
  }

}
