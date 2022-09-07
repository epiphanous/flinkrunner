package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.util.Try

/** A thin wrapper to emit an embedded avro record from events into an
  * output stream in a json (lines) format.
  *
  * @param pretty
  *   true if you want to indent the json code (default = false)
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
    with JsonCodec
    with LazyLogging {

  lazy val lineEndBytes: Array[Byte] =
    System.lineSeparator().getBytes(StandardCharsets.UTF_8)

  def asMap(record: A): util.Map[String, AnyRef] =
    record.getSchema.getFields.asScala.toList
      .map { f =>
        val name = f.name()
        name -> record.get(name)
      }
      .toMap
      .asJava

  override def encode(element: E, stream: OutputStream): Unit =
    Try(getMapper().writeValueAsBytes(asMap(element.$record)))
      .fold(
        t => logger.error(s"failed to encode json $element", t),
        bytes => {
          stream.write(bytes)
          stream.write(lineEndBytes)
        }
      )

}
