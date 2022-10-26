package io.epiphanous.flinkrunner.serde
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** Encoder for writing an element to a delimited text file output stream
  *
  * @param delimitedConfig
  *   a configuration of delimited file parameters
  * @tparam E
  *   the type to encode into the file
  */
class DelimitedFileEncoder[E: TypeInformation](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends Encoder[E] {

  @transient
  lazy val encoder: DelimitedRowEncoder[E] =
    new DelimitedRowEncoder[E](delimitedConfig)

  @transient
  var out: OutputStream = _

  override def encode(element: E, stream: OutputStream): Unit = {
    if (out != stream) {
      out = stream
      encoder.codec.maybeWriteHeader(stream)
    }
    encoder
      .encode(element)
      .fold(
        err =>
          throw new RuntimeException(
            s"failed to delimited-encode $element",
            err
          ),
        line => stream.write(line.getBytes(StandardCharsets.UTF_8))
      )
  }
}
