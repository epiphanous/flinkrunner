package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream

/** A thin wrapper to emit an embedded avro record from events into an
  * output stream in a delimited format.
  *
  * @param delimitedConfig
  *   the delimited file configuration
  * @tparam E
  *   the ADT event type that embeds an avro record of type A
  * @tparam A
  *   the avro type embedded in the ADT event
  * @tparam ADT
  *   the flink event algebraic data type
  */
class EmbeddedAvroDelimitedFileEncoder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)
    extends Encoder[E] {

  @transient
  lazy val avroDelimitedFileEncoder =
    new DelimitedFileEncoder[A](delimitedConfig)

  override def encode(element: E, stream: OutputStream): Unit =
    avroDelimitedFileEncoder.encode(element.$record, stream)
}
