package io.epiphanous.flinkrunner.serde
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{
  MapperFeature,
  ObjectMapper,
  ObjectWriter,
  SerializationFeature
}

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
    extends Encoder[E] {

  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper: ObjectMapper = new ObjectMapper()
    .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, sortKeys)
    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, sortKeys)
    .configure(SerializationFeature.INDENT_OUTPUT, pretty)

  @transient
  lazy val writer: ObjectWriter = mapper.writerFor(klass)

  override def encode(element: E, stream: OutputStream): Unit =
    stream.write(
      (writer.writeValueAsString(element) + System.lineSeparator())
        .getBytes(StandardCharsets.UTF_8)
    )
}
