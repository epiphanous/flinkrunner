package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.{ObjectWriter, SequenceWriter}
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.collection.JavaConverters._
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
    extends Encoder[E]
    with DelimitedCodec {

  @transient
  lazy val writer: ObjectWriter =
    getWriter[A](
      delimitedConfig,
      implicitly[TypeInformation[A]].getTypeClass
    )

  @transient
  var sequenceWriter: SequenceWriter = _

  @transient
  var lastStream: OutputStream = _

  override def encode(element: E, stream: OutputStream): Unit = {

    /** this is here to support writing headers properly */
    if (stream != lastStream) {
      if (Option(sequenceWriter).nonEmpty) {
        sequenceWriter.close()
        sequenceWriter = null
      }
      lastStream = stream
    }
    if (Option(sequenceWriter).isEmpty) {
      sequenceWriter = writer.writeValues(stream)
    }

    val record = element.$record
    val schema = record.getSchema
    val data   =
      schema.getFields.asScala
        .map { f =>
          val key = f.name()
          (key, record.get(f.name()))
        }
        .toMap
        .asJava

    sequenceWriter.write(data)
  }
}
