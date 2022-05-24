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
      |    bootstrap.servers = "kafka:9092"
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

  def getSerializerFor[E <: MyAvroADT](e: E) =
    new ConfluentAvroRegistryKafkaRecordSerializationSchema[E](
      config
        .getSinkConfig("test")
        .asInstanceOf[KafkaSinkConfig], // sink name must match this
      config,
      Some(schemaRegistryClient)
    ) {
      override def toKV(element: E): (Option[Any], Any) = {
        element match {
          case aw: AWrapper => (Some(aw.$id), aw.value)
          case bw: BWrapper => (Some(bw.$id), bw.value)
        }
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
    println(schemaInfo.getSchema)
    println(schemaInfo.getId)
    val schema      = new Schema.Parser().parse(schemaInfo.getSchema)
    val schemaId    = schemaInfo.getId
    val baos        = new ByteArrayOutputStream()
    val dos         = new DataOutputStream(baos)
    dos.writeByte(0)
    dos.writeInt(schemaId)
    dos.flush()
    val encoder     = EncoderFactory.get().binaryEncoder(baos, null)
    val datumWriter = new GenericDatumWriter[T](schema)
    datumWriter.write(obj, encoder)
    encoder.flush()
    val bytes       = baos.toByteArray
    baos.close()
    println(bytes.mkString("   encoded:", "|", ""))
    bytes
  }

  // some test fixtures
  val aName: String = className(genOne[ARecord])
  val bName: String = className(genOne[BRecord])

  val stringSchema: AvroSchema = new AvroSchema("""{"type":"string"}""")
  val schemaRegistryClient     = config.getSchemaRegistryClient
  schemaRegistryClient.register(
    s"test-key",
    stringSchema,
    true
  )
  schemaRegistryClient.register(
    aName,
    new AvroSchema(ARecord.SCHEMA$),
    true
  )
  schemaRegistryClient.register(
    bName,
    new AvroSchema(BRecord.SCHEMA$),
    true
  )

  val keySchemaInfo: SchemaMetadata =
    schemaRegistryClient.getLatestSchemaMetadata("test-key")
  val aSchemaInfo: SchemaMetadata   =
    schemaRegistryClient.getLatestSchemaMetadata(
      aName
    )
  val bSchemaInfo: SchemaMetadata   =
    schemaRegistryClient.getLatestSchemaMetadata(
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
    val e                                     = genOne[BWrapper]
//    val b = genOne[BWrapper]
    val serializer                            = getSerializerFor(e)
    val (key, value)                          = serializer.toKV(e)
    println(key)
    println(value)
    val expectedKeyBytes: Option[Array[Byte]] =
      key.map(k => binaryEncode(k, keySchemaInfo))
    val expectedValueBytes: Array[Byte]       =
      binaryEncode(value, bSchemaInfo)
    val serialized                            = serializer.serialize(e, null, e.$timestamp)
    println(serialized.value().mkString("serialized:", "|", ""))
    //      serialized.key() shouldEqual expectedKeyBytes.value
    serialized.value() shouldEqual expectedValueBytes
//      serialized.timestamp() shouldEqual event.$timestamp
//      serialized.topic() shouldEqual serializer.topic

  }

}
