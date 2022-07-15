package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{
  MockSchemaRegistryClient,
  SchemaMetadata,
  SchemaRegistryClient
}
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{
  GenericContainer,
  GenericDatumWriter,
  GenericRecord
}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.flink.api.scala.createTypeInformation
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.{ByteArrayOutputStream, DataOutputStream}

trait SerdeTestFixtures extends PropSpec {
  val optConfig: String =
    s"""
       |jobs {
       |  DeduplicationJob {
       |    sourceNames = [ observations ]
       |    sinkNames = [ test ]
       |  }
       |}
       |sources {
       |  test {
       |    connector = kafka
       |    topic = test
       |    isKeyed = true
       |    bootstrap.servers = "kafka:9092"
       |    schema.registry {
       |      url = "mock://test"
       |      avro.use.logical.type.converters = true
       |      avro.remove.java.properties = true
       |    }
       |  }
       |}
       |sinks {
       |  test {
       |    connector = kafka
       |    topic = test
       |    isKeyed = true
       |    bootstrap.servers = "kafka:9092"
       |    schema.registry {
       |      url = "mock://test"
       |      avro.use.logical.type.converters = true
       |      avro.remove.java.properties = true
       |    }
       |  }
       |}
       |""".stripMargin
  val runner            =
    getRunner[MyAvroADT](Array("confluent-serde-test"), Some(optConfig))

  val kafkaSinkConfig: KafkaSinkConfig[MyAvroADT] =
    runner.getSinkConfig("test").asInstanceOf[KafkaSinkConfig[MyAvroADT]]

  val aWrapper: AWrapper = genOne[AWrapper]
  val bWrapper: BWrapper = genOne[BWrapper]

  val aName: String = className(genOne[ARecord])
  val bName: String = className(genOne[BRecord])

  val stringSchema: AvroSchema                   = new AvroSchema("""{"type":"string"}""")
  val schemaRegistryClient: SchemaRegistryClient =
    new MockSchemaRegistryClient()
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

  def getSerializerFor[
      E <: MyAvroADT with EmbeddedAvroRecord[A],
      A <: GenericRecord] = {
    val ss = new ConfluentAvroRegistryKafkaRecordSerializationSchema[
      E,
      A,
      MyAvroADT
    ](
      kafkaSinkConfig,
      Some(schemaRegistryClient)
    )
    ss.open(null, null)
    ss
  }

  val kafkaSourceConfig: KafkaSourceConfig[MyAvroADT] =
    runner
      .getSourceConfig("test")
      .asInstanceOf[KafkaSourceConfig[MyAvroADT]]

  def getDeserializerFor[
      E <: MyAvroADT with EmbeddedAvroRecord[A],
      A <: GenericRecord](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E) = {
    val ds = new ConfluentAvroRegistryKafkaRecordDeserializationSchema[
      E,
      A,
      MyAvroADT
    ](
      kafkaSourceConfig,
      Some(schemaRegistryClient)
    )
    ds.open(null)
    ds
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
    bytes
  }

  def binaryDecode[T >: Null <: GenericContainer](
      bytes: Array[Byte],
      schemaInfo: SchemaMetadata): T = {
    val schema      = new Schema.Parser().parse(schemaInfo.getSchema)
    val datumReader = new SpecificDatumReader[T](schema)
    val decoder     = DecoderFactory.get().binaryDecoder(bytes, null)
    datumReader.read(null, decoder)
  }

  val aKeyBytes       = binaryEncode(aWrapper.$recordKey.get, keySchemaInfo)
  val aValueBytes     = binaryEncode(aWrapper.$record, aSchemaInfo)
  val aConsumerRecord = new ConsumerRecord(
    kafkaSourceConfig.topic,
    1,
    1,
    aKeyBytes,
    aValueBytes
  )
  val bKeyBytes       = binaryEncode(bWrapper.$recordKey.get, keySchemaInfo)
  val bValueBytes     = binaryEncode(bWrapper.$record, bSchemaInfo)
  val bConsumerRecord = new ConsumerRecord(
    kafkaSourceConfig.topic,
    1,
    1,
    bKeyBytes,
    bValueBytes
  )

}
