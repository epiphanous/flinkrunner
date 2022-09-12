package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.sink.SinkConfig
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.nio.charset.StandardCharsets

/** Serialize an event instance into a json-encoded byte array.
  *
  * @param sinkConfig
  *   config for the sink we're serializing to
  * @tparam E
  *   the event type member of the flink ADT
  * @tparam ADT
  *   The flink event algebraic data type
  */
class JsonSerializationSchema[
    E <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](sinkConfig: SinkConfig[ADT])
    extends SerializationSchema[E]
    with LazyLogging {

  val name: String           = sinkConfig.name
  val jsonConfig: JsonConfig = JsonConfig(
    sinkConfig.properties.getProperty("json.pretty", "false").toBoolean,
    sinkConfig.properties.getProperty("json.sort.keys", "false").toBoolean,
    Option(
      sinkConfig.properties.getProperty("json.eol", System.lineSeparator())
    ).map(s => if (s.equalsIgnoreCase("none")) null else s)
  )
  val jsonRowEncoder         = new JsonRowEncoder[E](jsonConfig)

  /** Serialize an event into json-encoded byte array
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
            s"failed to serialize event to sink $name ${error.getMessage}\nEVENT:\n$event\n"
          ),
        _.getBytes(StandardCharsets.UTF_8)
      )
  }

}
