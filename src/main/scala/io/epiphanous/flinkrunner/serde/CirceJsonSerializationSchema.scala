package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.syntax._
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  SinkConfig
}
import org.apache.flink.api.common.serialization.SerializationSchema

import java.nio.charset.StandardCharsets

/**
 * A JSON serialization schema that uses the circe json library.
 *
 * @param sinkName
 *   name of the sink we're serializing to
 * @param config
 *   a flink runner config
 * @param circeEncoder
 *   an implicit circe encoder
 * @tparam E
 *   the ADT member type we're serializing
 * @tparam ADT
 *   the flink runner ADT
 */
class CirceJsonSerializationSchema[ADT <: FlinkEvent](
    sinkName: String,
    config: FlinkConfig[ADT])(implicit circeEncoder: Encoder[ADT])
    extends SerializationSchema[ADT]
    with LazyLogging {

  val sourceConfig: SinkConfig = config.getSinkConfig(sinkName)
  val configPretty: Boolean    =
    sourceConfig.properties.getProperty("pretty", "false").toBoolean
  val configSort: Boolean      =
    sourceConfig.properties.getProperty("sort", "false").toBoolean

  /**
   * Serialize an ADT event into json byte array
   * @param event
   *   an instance of an ADT event type
   * @return
   *   a json encoded byte array
   */
  override def serialize(event: ADT): Array[Byte] =
    toJson(event).getBytes(StandardCharsets.UTF_8)

  /**
   * Utility method to convert an event into a JSON string with options for
   * pretty-printing and sorting keys
   * @param event
   *   the ADT event instance to encode
   * @param pretty
   *   true to encode with lines and 2 space indentation
   * @param sortKeys
   *   true to sort the json keys
   * @return
   *   a json-encoded string
   */
  def toJson(
      event: ADT,
      pretty: Boolean = configPretty,
      sortKeys: Boolean = configSort): String = {
    val j = event.asJson
    if (pretty) {
      if (sortKeys) j.spaces2SortKeys else j.spaces2
    } else {
      if (sortKeys) j.noSpacesSortKeys else j.noSpaces
    }
  }
}
