package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec
import org.apache.avro.generic.GenericRecord

import java.util.UUID

class AssignKafkaHeadersSpec extends PropSpec {

  case class MyEventWithHeaderFields[A <: GenericRecord](
      $record: A,
      override val $recordHeaders: Map[String, String] = Map.empty,
      override val customAssignableHeaders: Seq[KafkaHeaderMapper] =
        Seq.empty,
      $id: String = UUID.randomUUID().toString,
      $key: String = "no-headers",
      $timestamp: Long = 1L)
      extends FlinkEvent
      with EmbeddedAvroRecord[A]
      with AssignKafkaHeaders[A]

  property("works with any standard headers") {
    val rec: RecordWithHeaders = RecordWithHeaders("some string", 17)
    val event                  = MyEventWithHeaderFields[RecordWithHeaders](
      rec,
      Map(
        "Kafka.Offset" -> "123",
        "Kafka.Topic"  -> "blah"
      )
    )
    // println(s"$rec")
    rec.kafka_offset shouldEqual Some(123L)
    rec.kafka_topic shouldEqual Some("blah")
    rec.kafka_partition shouldEqual None
  }

  property("works with no headers") {
    val rec: RecordWithHeaders = RecordWithHeaders("some string", 17)
    val event                  = MyEventWithHeaderFields[RecordWithHeaders](
      rec
    )
    // println(s"$rec")
    rec.kafka_offset shouldEqual None
    rec.kafka_topic shouldEqual None
    rec.kafka_partition shouldEqual None
  }

  property("works with custom headers") {
    val rec: RecordWithHeaders = RecordWithHeaders("some string", 17)
    val event                  = MyEventWithHeaderFields[RecordWithHeaders](
      rec,
      Map(
        "someIntHeader" -> "36"
      ),
      Seq(
        KafkaHeaderMapper(
          "someIntHeader",
          "myIntHeader",
          (s: String) => Option(s).map(_.toInt)
        )
      )
    )
//    println(s"$rec")
    rec.myIntHeader shouldEqual Some(36)
  }

}
