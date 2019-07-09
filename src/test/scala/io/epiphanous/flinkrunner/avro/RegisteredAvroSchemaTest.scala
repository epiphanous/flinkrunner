package io.epiphanous.flinkrunner.avro

import java.nio.ByteBuffer
import java.time.Instant

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.file.DataFileConstants
import org.scalatest.{FlatSpec, Matchers, TryValues}

class RegisteredAvroSchemaTest extends FlatSpec with Matchers with TryValues with LazyLogging {
  behavior of "RegisteredAvroSchemaTest"

  val regSchema =
    RegisteredAvroSchema(17, AvroSchema[TestAvroClass1], "io_epiphanous_flinkrunner_avro_test_class_value", 1)

  val testObj = TestAvroClass1.obj1

  logger.debug(regSchema.schema.toString)

  it should "encode with magic" in {
    val encoded = regSchema
      .encode(testObj)
    encoded.isSuccess shouldBe true
    val result = encoded.success.value.slice(0, 5)
    val expected = ByteBuffer.allocate(5).put(RegisteredAvroSchema.MAGIC).putInt(regSchema.id).array()
    result.shouldEqual(expected)
  }

  it should "encode without magic" in {
    val encoded = regSchema
      .encode(testObj, false)
    encoded.isSuccess shouldBe true
    val magic = DataFileConstants.MAGIC
    val result = encoded.success.value.slice(0, magic.length)
    result.shouldEqual(magic)
  }

  it should "encode and decode" in {
    val roundTrip = regSchema
      .encode(testObj, false)
      .flatMap[TestAvroClass1](bytes => regSchema.decode[TestAvroClass1](ByteBuffer.wrap(bytes)))
    logger.debug(roundTrip.toString)
    roundTrip.isSuccess shouldBe true
    roundTrip.success.value.shouldEqual(testObj)
  }

}
