package io.epiphanous.flinkrunner.avro

import java.time.Instant

case class TestAvroClass1(x: Int, y: Either[String, Int], z: Option[Double], t: Instant, a: TestAvroClass2)
object TestAvroClass1 {
  val obj1 = TestAvroClass1(1, Left("hello"), Some(1.1), Instant.now(), TestAvroClass2("yaya"))
}
