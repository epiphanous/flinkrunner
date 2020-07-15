package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.avro.RegisteredAvroSchema._
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroCoderTest extends AnyFlatSpec with Matchers with TryValues with LazyLogging {
  behavior of "AvroCoderTest"

  val obj = TestAvroClass1.obj1
  val registry = new TestAvroSchemaRegistryClient()
  val regSchema = RegisteredAvroSchema(1, AvroSchema[TestAvroClass1], registry.subject(obj, false), 1)
  registry.install(regSchema)

  val coder = new AvroCoder(registry)

  it should "encode and decode" in {
    val result = coder
      .encode(obj)
      .flatMap(bytes => coder.decode[TestAvroClass1](bytes))
    result.isSuccess shouldBe true
    logger.debug(s"$obj")
    logger.debug(s"${result.success.value}")
    result.success.value shouldEqual obj
  }

}
