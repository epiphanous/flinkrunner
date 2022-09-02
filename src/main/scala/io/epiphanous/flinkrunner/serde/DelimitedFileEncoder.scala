package io.epiphanous.flinkrunner.serde
import com.fasterxml.jackson.databind.ObjectWriter
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream

/** Encoder for writing an element to a delimited text file output stream
  *
  * @param delimitedConfig
  *   a configuration of delimited file parameters
  * @tparam E
  *   the type to encode into the file
  */
class DelimitedFileEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends Encoder[E]
    with DelimitedCodec {

  @transient
  lazy val typeClass: Class[E] =
    implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val header: Array[Byte] = getHeader(delimitedConfig, typeClass)

  @transient
  lazy val writer: ObjectWriter =
    getWriter(delimitedConfig, typeClass)

  @transient
  var out: OutputStream = _

  override def encode(element: E, stream: OutputStream): Unit = {
    if (delimitedConfig.useHeader) {
      if (out != stream) {
        out = stream
        stream.write(header)
      }
    }
    stream.write(writer.writeValueAsBytes(element))
  }
}
