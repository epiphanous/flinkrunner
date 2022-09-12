package io.epiphanous.flinkrunner.serde
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** Encoder for writing an element to a json text file output stream
  *
  * @param jsonConfig
  *   a Json encoder config
  * @tparam E
  *   the type to encode into the file
  */
class JsonFileEncoder[E: TypeInformation](
    jsonConfig: JsonConfig = JsonConfig())
    extends Encoder[E]
    with LazyLogging {

  @transient
  lazy val rowEncoder = new JsonRowEncoder[E](jsonConfig)

  override def encode(element: E, stream: OutputStream): Unit = {
    rowEncoder
      .encode(element)
      .fold(
        t => logger.error(s"failed to json-encode $element", t),
        s => stream.write(s.getBytes(StandardCharsets.UTF_8))
      )
  }
}
