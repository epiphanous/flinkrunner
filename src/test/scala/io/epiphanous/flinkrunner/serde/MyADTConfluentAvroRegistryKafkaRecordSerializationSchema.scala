package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{
  MockSchemaRegistryClient,
  SchemaRegistryClient
}
import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  BRecord,
  BWrapper,
  FlinkConfig,
  MyAvroADT
}
import org.apache.avro.specific.SpecificRecord

class MyADTConfluentAvroRegistryKafkaRecordSerializationSchema[
    E <: MyAvroADT](name: String, config: FlinkConfig[MyAvroADT])
    extends ConfluentAvroRegistryKafkaRecordSerializationSchema[
      E,
      MyAvroADT](
      name,
      config
    ) {

  /**
   * Implementing subclasses must provide an instance of a schema registry
   * client to use, for instance a <code>CachedSchemaRegistryClient</code>
   * or a <code>MockSchemaRegistryClient</code> for testing.
   */
  override val schemaRegistryClient: SchemaRegistryClient =
    new MockSchemaRegistryClient()

  // for testing purposes
  val stringSchema: AvroSchema = new AvroSchema("""{"type":"string"}""")
  val aRecordName: String      =
    ARecord.getClass.getCanonicalName.replaceAll("\\$$", "")
  val bRecordName: String      =
    BRecord.getClass.getCanonicalName.replaceAll("\\$$", "")
  schemaRegistryClient.register(
    s"test-key",
    stringSchema
  )
  schemaRegistryClient.register(
    aRecordName,
    new AvroSchema(ARecord.SCHEMA$)
  )
  schemaRegistryClient.register(
    bRecordName,
    new AvroSchema(BRecord.SCHEMA$)
  )

  /**
   * Map a flinkrunner ADT instance into a key/value pair to serialize into
   * kafka
   * @param element
   *   an instance of the flinkrunner ADT
   * @return
   *   (Option[AnyRef], AnyRef)
   */
  override def toKeyValue(element: E): (Option[String], SpecificRecord) =
    element match {
      case a: AWrapper =>
        (Some(a.$id), a.value)
      case b: BWrapper =>
        (Some(b.$id), b.value)
    }
}
