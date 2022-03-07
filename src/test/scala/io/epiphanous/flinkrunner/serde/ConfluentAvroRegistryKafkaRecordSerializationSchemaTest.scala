package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory

import java.io.{ByteArrayOutputStream, DataOutputStream}
import scala.language.higherKinds

class ConfluentAvroRegistryKafkaRecordSerializationSchemaTest
    extends PropSpec {
  val optConfig: String =
    s"""
      |jobs {
      |  DeduplicationJob {
      |    sourceNames = [ observations ]
      |    sinkNames = [ test ]
      |  }
      |}
      |sources {
      |  observations {
      |    connector = collection
      |  }
      |}
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
  val config            = new FlinkConfig(
    Array.empty[String],
    Some(optConfig)
  )

  def getSerializerFor[E <: MyAvroADT](
      event: E): ConfluentAvroRegistryKafkaRecordSerializationSchema[E] =
    new ConfluentAvroRegistryKafkaRecordSerializationSchema[E](
      config
        .getSinkConfig("test")
        .asInstanceOf[KafkaSinkConfig], // sink name must match this
      config
    ) {
      override def toKV(element: E): (Option[AnyRef], AnyRef) =
        element match {
          case aw: AWrapper => (Some(aw.$id), aw.value)
          case bw: BWrapper => (Some(bw.$id), bw.value)
        }

      override def eventTime(element: E, timestamp: Long): Long =
        element.$timestamp
    }

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
  val aName: String = className(genOne[ARecord])
  val bName: String = className(genOne[BRecord])

  val stringSchema: AvroSchema = new AvroSchema("""{"type":"string"}""")
  config.schemaRegistryClient.register(
    s"test-key",
    stringSchema
  )
  config.schemaRegistryClient.register(
    aName,
    new AvroSchema(ARecord.SCHEMA$)
  )
  config.schemaRegistryClient.register(
    bName,
    new AvroSchema(BRecord.SCHEMA$)
  )

  val keySchemaInfo: SchemaMetadata =
    config.schemaRegistryClient.getLatestSchemaMetadata("test-key")
  val aSchemaInfo: SchemaMetadata   =
    config.schemaRegistryClient.getLatestSchemaMetadata(
      aName
    )
  val bSchemaInfo: SchemaMetadata   =
    config.schemaRegistryClient.getLatestSchemaMetadata(
      bName
    )

  property("find the right schema for a key") {
    keySchemaInfo.getSchema shouldEqual "\"string\""
  }

  property("find the right schema for a value class") {
    aSchemaInfo.getSchema shouldEqual ARecord.SCHEMA$.toString
    bSchemaInfo.getSchema shouldEqual BRecord.SCHEMA$.toString
  }

  property("serialize a MyAvroADT instance to a producer record") {
    forAll { (event: MyAvroADT) =>
      val serializer                            = getSerializerFor(event)
      val (key, value)                          = serializer.toKV(event)
      val expectedKeyBytes: Option[Array[Byte]] =
        key.map(k => binaryEncode(k, keySchemaInfo))
      val schemaInfo                            = event match {
        case _: AWrapper => aSchemaInfo
        case _: BWrapper => bSchemaInfo
      }
      val expectedValueBytes: Array[Byte]       = binaryEncode(value, schemaInfo)
      val serialized                            = serializer.serialize(event, null, event.$timestamp)
      serialized.key() shouldEqual expectedKeyBytes.value
      serialized.value() shouldEqual expectedValueBytes
      serialized.timestamp() shouldEqual event.$timestamp
      serialized.topic() shouldEqual serializer.topic
    }
  }

}
