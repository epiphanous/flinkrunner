package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  MockSchemaRegistryClient
}
import org.apache.flink.formats.avro.SchemaCoder
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder

import java.util

case class ConfluentSchemaCoderProvider(
    schemaRegistryUrl: String,
    schemaRegistryProps: util.HashMap[String, String],
    subject: Option[String] = None,
    cacheCapacity: Int = 1000,
    useMockClient: Boolean = false)
    extends SchemaCoder.SchemaCoderProvider {
  override def get(): SchemaCoder = {
    val registryClient =
      if (useMockClient) new MockSchemaRegistryClient()
      else
        new CachedSchemaRegistryClient(
          schemaRegistryUrl,
          cacheCapacity,
          schemaRegistryProps
        )
    new ConfluentSchemaRegistryCoder(subject.orNull, registryClient)
  }
}
