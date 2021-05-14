package io.epiphanous.flinkrunner.avro

case class ConfluentSchemaRegistryContext(
    isKey: Boolean = false,
    version: String = "latest")
