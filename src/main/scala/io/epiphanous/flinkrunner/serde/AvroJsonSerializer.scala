package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/** A simple custom jackson serializer to handle serializing avro records
  * (Generic or Specific)
  */
class AvroJsonSerializer
    extends StdSerializer[GenericRecord](classOf[GenericRecord])
    with LazyLogging {
  override def serialize(
      record: GenericRecord,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    record.getSchema.getFields.asScala.foreach { f =>
      _serializeAvroValue(
        f.name(),
        record.get(f.name()),
        f.schema(),
        gen,
        provider
      )
    }
    gen.writeEndObject()
  }

  @tailrec
  private def _serializeAvroValue[T: TypeInformation](
      name: String,
      value: T,
      schema: Schema,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    (schema.getType, value) match {
      case (NULL, _)                             => gen.writeNullField(name)
      case (_, null | None)                      => gen.writeNullField(name)
      case (_, Some(v))                          =>
        _serializeAvroValue(name, v, schema, gen, provider)
      case (RECORD, record: GenericRecord)       =>
        gen.writeFieldName(name)
        serialize(record, gen, provider)
      case (ENUM, ord: Int)                      =>
        gen.writeStringField(name, schema.getEnumSymbols.get(ord))
      case (ARRAY, seq: Seq[_])                  =>
        gen.writeArrayFieldStart(name)
        seq.foreach { e =>
          _serializeElement(e, schema.getElementType, gen, provider)
        }
        gen.writeEndArray()
      case (MAP, map: Map[String, _] @unchecked) =>
        gen.writeObjectFieldStart(name)
        map.foreach { case (k, e) =>
          gen.writeFieldName(k)
          _serializeElement(e, schema.getValueType, gen, provider)
        }
        gen.writeEndObject()
      case (UNION, _)                            =>
        // todo: not a very sophisticated way to process unions, but it covers common case of [null, type]
        val nonNullTypes =
          schema.getTypes.asScala.filterNot(s => s.getType == NULL)
        if (nonNullTypes.size > 1) {
          throw new RuntimeException(
            s"field $name of type union has more than one non-null types: $nonNullTypes"
          )
        }
        _serializeAvroValue(
          name,
          value,
          nonNullTypes.head,
          gen,
          provider
        )
      case (FIXED | BYTES, bytes: Array[Byte])   =>
        gen.writeBinaryField(name, bytes)
      case (STRING, string: String)              =>
        gen.writeStringField(name, string)
      case (INT, int: Int)                       =>
        gen.writeNumberField(name, int)
      case (LONG, long: Long)                    => gen.writeNumberField(name, long)
      case (FLOAT, float: Float)                 => gen.writeNumberField(name, float)
      case (DOUBLE, double: Double)              => gen.writeNumberField(name, double)
      case (BOOLEAN, boolean: Boolean)           =>
        gen.writeBooleanField(name, boolean)
      case _                                     =>
        gen.writeFieldName(name)
        provider
          .findValueSerializer(
            implicitly[TypeInformation[T]].getTypeClass
          )
          .asInstanceOf[JsonSerializer[T]]
          .serialize(value, gen, provider)
    }
  }

  private def _serializeElement(
      value: Any,
      schema: Schema,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    (schema.getType, value) match {
      case (_, null | None)                => gen.writeNull()
      case (_, Some(v))                    => _serializeElement(v, schema, gen, provider)
      case (RECORD, record: GenericRecord) =>
        serialize(record, gen, provider)
      case (ENUM, ord: Int)                =>
        gen.writeString(schema.getEnumSymbols.get(ord))
      case (ARRAY, seq: Seq[_])            =>
        seq.foreach { e =>
          _serializeElement(e, schema.getElementType, gen, provider)
        }
      case (MAP, _)                        => gen.writeObject(value)
      case (UNION, _)                      => // todo
      case (STRING, string: String)        => gen.writeString(string)
      case (INT, int: Int)                 => gen.writeNumber(int)
      case (LONG, long: Long)              => gen.writeNumber(long)
      case (DOUBLE, double: Double)        => gen.writeNumber(double)
      case (BOOLEAN, boolean: Boolean)     => gen.writeBoolean(boolean)
      case _                               =>
        logger.error(
          s"no serializer for array element type ${schema.getType.name()}"
        )
      // todo
    }
  }
}
