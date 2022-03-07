package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.syntax._
import org.apache.flink.api.common.serialization.SerializationSchema

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
class CirceJsonSerializationSchema[E](
    sinkName: String = "unknown",
    pretty: Boolean = false,
    sortKeys: Boolean = false)(implicit circeEncoder: Encoder[E])
    extends SerializationSchema[E]
    with LazyLogging {

  /**
   * Serialize an event into json-encoded byte array
   * @param event
   *   an event instance
   * @return
   *   a json encoded byte array
   */
  override def serialize(event: E): Array[Byte] =
    toJson(event).getBytes(StandardCharsets.UTF_8)

  /**
   * Utility method to convert an event into a JSON string with options for
   * pretty-printing and sorting keys
   * @param event
   *   the event instance to encode
   * @param _pretty
   *   true to encode with lines and 2 space indentation
   * @param _sortKeys
   *   true to sort the json keys
   * @return
   *   a json-encoded string
   */
  def toJson(
      event: E,
      _pretty: Boolean = pretty,
      _sortKeys: Boolean = sortKeys): String = {
    val j = event.asJson
    if (_pretty) {
      if (_sortKeys) j.spaces2SortKeys else j.spaces2
    } else {
      if (_sortKeys) j.noSpacesSortKeys else j.noSpaces
    }
  }
}
