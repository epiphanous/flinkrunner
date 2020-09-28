package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestAvroSchemaRegistryClientTest extends AnyFlatSpec with Matchers with TryValues with LazyLogging {

  val registry = new TestAvroSchemaRegistryClient()
  val testObj = TestAvroClass1.obj1

  behavior of "TestAvroSchemaRegistryClientTest"

  it should "subject for key" in {
    registry.subject(testObj, true) shouldEqual "io.epiphanous.flinkrunner.avro.TestAvroClass1-key"
  }

  it should "subject for value" in {
    registry.subject(testObj, false) shouldEqual "io.epiphanous.flinkrunner.avro.TestAvroClass1-value"
  }

  it should "install and get" in {
    def runScenario(isKey: Boolean) {
      val subject = registry.subject(testObj, isKey)
      val regSchema = RegisteredAvroSchema(1, AvroSchema[TestAvroClass1], subject, 1)
      registry.schemas.isEmpty shouldBe true
      registry.install(regSchema)
      registry.schemas.size shouldBe 2
      val getById = registry.get(regSchema.id)
      getById.isSuccess shouldBe true
      getById.success.value shouldEqual regSchema
      val getBySubject = registry.get(subject)
      getBySubject.isSuccess shouldBe true
      getBySubject.success.value shouldEqual regSchema
      val getByObj = registry.get(testObj, isKey)
      getByObj.isSuccess shouldBe true
      getByObj.success.value shouldEqual regSchema
      registry.clear()
    }
    runScenario(false)
    runScenario(true)
  }

}
