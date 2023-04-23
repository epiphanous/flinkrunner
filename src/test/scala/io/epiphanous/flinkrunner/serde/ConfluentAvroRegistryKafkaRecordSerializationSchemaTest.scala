package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation

import java.time.Instant

class ConfluentAvroRegistryKafkaRecordSerializationSchemaTest
    extends SerdeTestFixtures {

  property("find the right schema for a key") {
    keySchemaInfo.getSchema shouldEqual "\"string\""
  }

  property("find the right schema for a value class") {
    aSchemaInfo.getSchema shouldEqual ARecord.SCHEMA$.toString
    bSchemaInfo.getSchema shouldEqual BRecord.SCHEMA$.toString
  }

  // ignore this until we set up testcontainers schema registry testing
  property("serialize a MyAvroADT instance to a producer record") {
    val serializer = getSerializerFor[BWrapper, BRecord]
    val serialized = serializer.serialize(
      bWrapper,
      null,
      Instant.now().toEpochMilli
    )
//    showBytes("serialized key:", serialized.key())
//    showBytes("serialized value:", serialized.value())
    serialized.key() shouldEqual bKeyBytes
    serialized.value() shouldEqual bValueBytes
    serialized.timestamp() shouldEqual bWrapper.$timestamp
  }

}
