package io.epiphanous.flinkrunner.avro

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
class AvroCodingException(
    message: String = "Failure during Avro coding",
    cause: Throwable = None.orNull)
    extends Exception(message, cause)
