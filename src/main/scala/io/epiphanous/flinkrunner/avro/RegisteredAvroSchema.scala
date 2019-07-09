package io.epiphanous.flinkrunner.avro

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, RecordFormat, SchemaFor, ToRecord}
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}

import scala.util.Try

case class RegisteredAvroSchema(id: Int, schema: Schema, subject: String = "", version: Int = 0) {
  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)

  def name = if (subject.isEmpty) schema.getFullName else subject

  def decode[E: Encoder: Decoder](buffer: ByteBuffer): Try[E] =
    Try({
      val is = new SeekableByteArrayInput(buffer.array().slice(buffer.position(), buffer.limit()))
      val dataFileReader = new DataFileReader[GenericRecord](is, datumReader)
      val datum = dataFileReader.next()
      dataFileReader.close()
      RecordFormat[E](schema).from(datum)
    })

  def encode[E: Encoder: Decoder](event: E, addMagic: Boolean = true): Try[Array[Byte]] =
    Try({
      val os = new ByteArrayOutputStream()
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      dataFileWriter.create(schema, os)
      val datum = RecordFormat[E](schema).to(event)
      dataFileWriter.append(datum)
      dataFileWriter.close()
      val bytes = os.toByteArray
      if (addMagic) {
        val size = bytes.length + 5 // 5 is magic + int id
        ByteBuffer.allocate(size).put(RegisteredAvroSchema.MAGIC).putInt(id).put(bytes).array()
      } else bytes
    })
}

object RegisteredAvroSchema {
  final val MAGIC = 0x0.toByte
}
