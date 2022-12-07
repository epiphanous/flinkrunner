package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers

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
//    logger.debug(s"serializing avro record: $record")
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

  /** Based on a value, determine which, if any, of the possible schemas in
    * a union apply to the value.
    * @param name
    *   name of the field which has the union type
    * @param value
    *   the value that needs serializing
    * @param unionSchemas
    *   the admissible schemas for the union
    * @tparam T
    *   the type of value
    * @return
    *   Option[Schema]
    */
  def findSchemaOf[T](
      name: String,
      value: T,
      unionSchemas: List[Schema]): Schema =
    (if (value == null) {

       unionSchemas
         .find(_.getType.name().equalsIgnoreCase("null"))

     } else {

       val valueClass     = value.getClass
       val valueClassName = valueClass.getCanonicalName

       def clsMatch[S](s: Schema, cls: Class[S]): Option[Schema] =
         if (
           cls.isAssignableFrom(valueClass) || valueClassName.equals(
             s.getFullName
           )
         ) Some(s)
         else None

       unionSchemas
         .flatMap(s =>
           s.getType match {
             case INT     =>
               clsMatch(s, classOf[Int]) orElse
                 clsMatch(
                   s,
                   classOf[java.lang.Integer]
                 )
             case DOUBLE  =>
               clsMatch(s, classOf[Double]) orElse clsMatch(
                 s,
                 classOf[java.lang.Double]
               )
             case FLOAT   =>
               clsMatch(s, classOf[Float]) orElse clsMatch(
                 s,
                 classOf[java.lang.Float]
               )
             case LONG    =>
               clsMatch(s, classOf[Long]) orElse clsMatch(
                 s,
                 classOf[java.lang.Long]
               )
             case BOOLEAN =>
               clsMatch(s, classOf[Boolean]) orElse clsMatch(
                 s,
                 classOf[java.lang.Boolean]
               )
             case STRING  => clsMatch(s, classOf[String])
             case ARRAY   =>
               clsMatch(s, classOf[Seq[_]]) orElse clsMatch(
                 s,
                 classOf[java.util.List[_]]
               )
             case MAP     =>
               clsMatch(s, classOf[collection.Map[_, _]]) orElse clsMatch(
                 s,
                 classOf[java.util.Map[_, _]]
               )
             case BYTES   =>
               clsMatch(s, classOf[ByteBuffer]) orElse clsMatch(
                 s,
                 classOf[Array[Byte]]
               )
             case ENUM    => clsMatch(s, classOf[GenericEnumSymbol[_]])
             case FIXED   => clsMatch(s, classOf[GenericFixed])
             case RECORD  => clsMatch(s, classOf[GenericRecord])
             case _       => None
           }
         )
         .headOption
     }).getOrElse(
      throw new RuntimeException(
        s"field $name has value ($value) of an unexpected type; should be in (${unionSchemas
            .map(_.getType.name())
            .mkString(", ")})"
      )
    )

  @tailrec
  private def _serializeAvroValue[T: TypeInformation](
      name: String,
      value: T,
      schema: Schema,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    (schema.getType, value) match {
      case (NULL, _) | (_, null | None)                       => gen.writeNullField(name)
      case (_, Some(v))                                       =>
        _serializeAvroValue(name, v, schema, gen, provider)
      case (RECORD, record: GenericRecord)                    =>
        gen.writeFieldName(name)
        serialize(record, gen, provider)
      case (ENUM, ord: Int)                                   =>
        gen.writeStringField(name, schema.getEnumSymbols.get(ord))
      case (ARRAY, seq: Seq[_])                               =>
        gen.writeArrayFieldStart(name)
        seq.zipWithIndex.foreach { case (e, i) =>
          _serializeElement(
            name,
            i.toString,
            e,
            schema.getElementType,
            gen,
            provider
          )
        }
        gen.writeEndArray()
      case (ARRAY, arr: Wrappers.MutableBufferWrapper[GenericRecord]) =>
        gen.writeArrayFieldStart(name)
        arr.asScala.zipWithIndex.foreach { case (e, i) =>
          _serializeElement(
            name,
            i.toString,
            e,
            schema.getElementType,
            gen,
            provider
          )
        }
        gen.writeEndArray()
      case (MAP, map: collection.Map[String, Any] @unchecked) =>
        gen.writeObjectFieldStart(name)
        map.foreach { case (k, e) =>
          gen.writeFieldName(k)
          _serializeElement(name, k, e, schema.getValueType, gen, provider)
        }
        gen.writeEndObject()
      case (UNION, _)                                         =>
        _serializeAvroValue(
          name,
          value,
          findSchemaOf(name, value, schema.getTypes.asScala.toList),
          gen,
          provider
        )
      case (FIXED | BYTES, bytes: Array[Byte])                => // TODO: test this
        gen.writeBinaryField(name, bytes)
      case (STRING, string: String)                           =>
        gen.writeStringField(name, string)
      case (INT, int: Int)                                    =>
        gen.writeNumberField(name, int)
      case (LONG, long: Long)                                 => gen.writeNumberField(name, long)
      case (FLOAT, float: Float)                              => gen.writeNumberField(name, float)
      case (DOUBLE, double: Double)                           => gen.writeNumberField(name, double)
      case (BOOLEAN, boolean: Boolean)                        =>
        gen.writeBooleanField(name, boolean)
      case _                                                  =>
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
      name: String,
      key: String,
      value: Any,
      schema: Schema,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    (schema.getType, value) match {
      case (_, null | None)                => gen.writeNull()
      case (_, Some(v))                    =>
        _serializeElement(name, key, v, schema, gen, provider)
      case (RECORD, record: GenericRecord) =>
        serialize(record, gen, provider)
      case (ENUM, ord: Int)                =>
        gen.writeString(schema.getEnumSymbols.get(ord))
      case (ARRAY, seq: Seq[_])            =>
        seq.zipWithIndex.foreach { case (e, i) =>
          _serializeElement(
            name,
            s"$key[$i]",
            e,
            schema.getElementType,
            gen,
            provider
          )
        }
      case (ARRAY, arr: Wrappers.MutableBufferWrapper[GenericRecord]) =>
        arr.asScala.zipWithIndex.foreach { case (e, i) =>
          _serializeElement(
            name,
            i.toString,
            e,
            schema.getElementType,
            gen,
            provider
          )
        }
      case (MAP, _)                        => gen.writeObject(value)
      case (UNION, _)                      =>
        _serializeElement(
          name,
          key,
          value,
          findSchemaOf(
            s"$name[$key]",
            value,
            schema.getTypes.asScala.toList
          ),
          gen,
          provider
        )
      case (STRING, string: String)        => gen.writeString(string)
      case (INT, int: Int)                 => gen.writeNumber(int)
      case (LONG, long: Long)              => gen.writeNumber(long)
      case (DOUBLE, double: Double)        => gen.writeNumber(double)
      case (BOOLEAN, boolean: Boolean)     => gen.writeBoolean(boolean)
      case _                               =>
        logger.error(
          s"no serializer found for $name[$key] element with type ${schema.getType
              .name()} and value $value"
        )
      // todo
    }
  }
}
