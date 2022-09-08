package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets

/** A JSON lines encoder for events with embedded avro records.
  *
  * @param pretty
  *   true if you want to indent the json output (default = false)
  * @param sortKeys
  *   true if you want to sort keys in the json output (default = false)
  * @tparam E
  *   the ADT event type that embeds an avro record of type A
  * @tparam A
  *   the avro type embedded in the ADT event
  * @tparam ADT
  *   the flink event algebraic data type
  */
class EmbeddedAvroJsonFileEncoder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](pretty: Boolean = false, sortKeys: Boolean = false)
    extends Encoder[E]
    with LazyLogging {

  @transient
  lazy val rowEncoder = new JsonRowEncoder[A](pretty, sortKeys)

  override def encode(element: E, stream: OutputStream): Unit =
    rowEncoder
      .encode(element.$record)
      .fold(
        t => logger.error(s"failed to json encode $element", t),
        s => stream.write(s.getBytes(StandardCharsets.UTF_8))
      )

}
