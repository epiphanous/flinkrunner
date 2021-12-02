package io.epiphanous.flinkrunner.avro

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
case class ConfluentSchemaRegistryContext(
    isKey: Boolean = false,
    version: String = "latest")
