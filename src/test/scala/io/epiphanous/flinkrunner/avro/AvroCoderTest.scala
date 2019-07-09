package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers, TryValues}

class AvroCoderTest extends FlatSpec with Matchers with TryValues with LazyLogging {
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
    result.success.value shouldEqual obj
  }

}
