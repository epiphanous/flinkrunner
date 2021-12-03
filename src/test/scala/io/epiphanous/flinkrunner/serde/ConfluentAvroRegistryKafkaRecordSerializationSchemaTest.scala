package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.avro.AvroSchema

import scala.language.higherKinds
import io.confluent.kafka.schemaregistry.client.{
  MockSchemaRegistryClient,
  SchemaMetadata
}
import io.epiphanous.flinkrunner.UnitSpec
import io.epiphanous.flinkrunner.model._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.time.Instant

class ConfluentAvroRegistryKafkaRecordSerializationSchemaTest
    extends UnitSpec {
  val factory              = new NoJobFactory[MyAvroADT]
  val optConfig: String    =
    s"""
      |sinks {
      |  test {
      |    connector = kafka
      |    topic = test
      |    isKeyed = true
      |    config {
      |      schema.registry.url = "mock://test"
      |      avro.use.logical.type.converters = true
      |      avro.remove.java.properties = true
      |      value.subject.name.strategy = io.confluent.kafka.serializers.subject.RecordNameStrategy
      |    }
      |  }
      |}
      |""".stripMargin
  val config               = new FlinkConfig[MyAvroADT](
    Array.empty[String],
    factory,
    Map.empty,
    Some(optConfig)
  )
  val schemaRegistryClient = new MockSchemaRegistryClient()

  val serde =
    new ConfluentAvroRegistryKafkaRecordSerializationSchema[MyAvroADT](
      "test", // sink name must match this
      config,
      schemaRegistryClient,
      toKV = {
        case a: AWrapper => (Some(a.$id), a.value)
        case b: BWrapper => (Some(b.$id), b.value)
      }
    )

  // helper to return the class name of the object passed in (without a $ at the end)
  def className[T](obj: T): String = {
    obj.getClass.getName match {
      case s if s.endsWith("$") => s.substring(0, s.length - 1)
      case s                    => s
    }
  }

  // printout a byte array with a prefix
  def showBytes(prefix: String, bytes: Array[Byte]): Unit =
    println(s"$prefix: ${bytes.mkString("Array(", ", ", ")")}")

  // mimic the binary encoding used for schema registry encoded objects
  def binaryEncode[T](obj: T, schemaInfo: SchemaMetadata): Array[Byte] = {
    val schema      = new Schema.Parser().parse(schemaInfo.getSchema)
    val schemaId    = schemaInfo.getId
    val baos        = new ByteArrayOutputStream()
    baos.write(0)
    val dos         = new DataOutputStream(baos)
    dos.writeInt(schemaId)
    dos.flush()
    val encoder     = EncoderFactory.get().binaryEncoder(baos, null)
    val datumWriter = new GenericDatumWriter[T](schema)
    datumWriter.write(obj, encoder)
    encoder.flush()
    val bytes       = baos.toByteArray
    baos.close()
    bytes
  }

  // some test fixtures
  val aRecord: ARecord   = ARecord("a123", 17, 32.2, Instant.now())
  val aWrapper: AWrapper = AWrapper(aRecord)
  val aName: String      = className(aRecord)

  val stringSchema: AvroSchema = new AvroSchema("""{"type":"string"}""")
  schemaRegistryClient.register(
    s"test-key",
    stringSchema
  )
  schemaRegistryClient.register(
    aName,
    new AvroSchema(ARecord.SCHEMA$)
  )

  val keySchemaInfo: SchemaMetadata =
    serde.schemaRegistryClient.getLatestSchemaMetadata("test-key")
  val aSchemaInfo: SchemaMetadata   =
    serde.schemaRegistryClient.getLatestSchemaMetadata(
      aName
    )

  behavior of "ConfluentAvroSerializationSchema"

  it should "find the right schema for a key" in {
    keySchemaInfo.getSchema shouldEqual "\"string\""
  }

  it should "find the right schema for a class" in {
    aSchemaInfo.getSchema shouldEqual ARecord.SCHEMA$.toString
  }

  it should "serialize to a producer record" in {
    val (aKey, aValue)                                = serde.toKV(aWrapper)
    val aWrapperKeyExpectedBytes: Option[Array[Byte]] =
      aKey.map(k => binaryEncode(k, keySchemaInfo))
    val aWrapperValueExpectedBytes: Array[Byte]       =
      binaryEncode(aValue, aSchemaInfo)
    val result                                        = serde.serialize(aWrapper, null, aWrapper.$timestamp)
    result.key() shouldEqual aWrapperKeyExpectedBytes.value
    result.value() shouldEqual aWrapperValueExpectedBytes
    result.timestamp() shouldEqual aWrapper.$timestamp
    result.topic() shouldEqual serde.topic
  }

}
