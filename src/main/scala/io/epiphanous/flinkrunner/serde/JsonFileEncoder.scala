package io.epiphanous.flinkrunner.serde
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{
  MapperFeature,
  ObjectMapper,
  ObjectWriter,
  SerializationFeature
}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** Encoder for writing an element to a json text file output stream
  *
  * @tparam E
  *   the type to encode into the file
  */
class JsonFileEncoder[E: TypeInformation](
    pretty: Boolean = false,
    sortKeys: Boolean = false)
    extends Encoder[E]
    with JsonCodec {

  @transient
  lazy val writer: ObjectWriter = getWriter(
    pretty,
    sortKeys,
    implicitly[TypeInformation[E]].getTypeClass
  )

  override def encode(element: E, stream: OutputStream): Unit =
    stream.write(
      (writer.writeValueAsString(element) + System.lineSeparator())
        .getBytes(StandardCharsets.UTF_8)
    )
}
