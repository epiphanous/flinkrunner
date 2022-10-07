package io.epiphanous.flinkrunner.model

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import java.util

case class SchemaRegistryConfig(
    url: String = "http://schema-registry:8082",
    cacheCapacity: Int = 1000,
    headers: util.HashMap[String, String] = new util.HashMap(),
    props: util.HashMap[String, String] = new util.HashMap()) {
  props.put("schema.registry.url", url)
  props.putIfAbsent("use.logical.type.converters", "true")
  props.putIfAbsent("specific.avro.reader", "true")
  props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false")
  def getClient: SchemaRegistryClient = {
    new CachedSchemaRegistryClient(
      url,
      cacheCapacity,
      props,
      headers
    )
  }
}
