package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BRecord, BWrapper, MyAvroADT}
import io.epiphanous.flinkrunner.util.AvroUtils.GenericToSpecific
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import java.time.Duration

class AvroUtilsTest extends PropSpec {

  def fromSpec(spec: BRecord): GenericRecord =
    new GenericRecordBuilder(BRecord.SCHEMA$)
      .set("b0", spec.b0)
      .set("b1", spec.b1.getOrElse(null)) // orNull won't work here
      .set("b2", spec.b2.getOrElse(null)) // orNull won't work here
      .set("b3", spec.b3.toEpochMilli)
      .build()

  property("isGeneric property") {
    AvroUtils.isGeneric(classOf[GenericRecord]) shouldEqual true
    AvroUtils.isGeneric(classOf[BRecord]) shouldEqual false
  }

  property("isSpecific property") {
    AvroUtils.isSpecific(classOf[GenericRecord]) shouldEqual false
    AvroUtils.isSpecific(classOf[BRecord]) shouldEqual true
  }

  property("GenericToSpecific property") {
    forAll { b: BRecord =>
      val g = fromSpec(b)
      val s = g.toSpecific(new BRecord())
      s shouldEqual b
    }
  }

  property("instanceOf property") {
    val x = AvroUtils.instanceOf(classOf[BRecord])
    val y = new BRecord()
    x.b0 shouldEqual y.b0
    x.b1 shouldEqual y.b1
    x.b2 shouldEqual y.b2
    Duration.between(x.b3, y.b3).toMillis should be <= 2L
  }

  property("toEmbeddedAvroInstance property") {
    val bw = genOne[BWrapper]
    val g  = fromSpec(bw.$record)
    val ea =
      AvroUtils.toEmbeddedAvroInstance[BWrapper, BRecord, MyAvroADT](
        g,
        classOf[BRecord]
      )
    ea shouldEqual bw
  }

}
