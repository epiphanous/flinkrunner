package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.AvroSchema
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.temporal.ChronoUnit

class AvroCoderTest
    extends AnyFlatSpec
    with Matchers
    with TryValues
    with LazyLogging {
  behavior of "AvroCoderTest"

  val obj       = TestAvroClass1.obj1
  val registry  = new TestSchemaRegistryClient()
  val subject   = registry.getSubjectName(obj)
  val regSchema = RegisteredAvroSchema(
    AvroSchema[TestAvroClass1],
    "1",
    Some(subject),
    Some("1")
  )
  registry.install(regSchema)

  val coder = new AvroCoder(registry)

  it should "encode and decode" in {
    val result = coder
      .encode(obj)
      .flatMap { bytes =>
        logger.debug(bytes.mkString(","))
        coder.decode[TestAvroClass1](bytes)
      }
//    result.isSuccess shouldBe true
    logger.debug(s"$obj")
    logger.debug(s"${result.success.value}")
    result.success.value shouldEqual obj.copy(t =
      obj.t.truncatedTo(ChronoUnit.MILLIS)
    )
  }

}
