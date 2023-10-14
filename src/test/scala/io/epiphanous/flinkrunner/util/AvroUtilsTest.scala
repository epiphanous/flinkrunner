package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.AvroUtils.RichGenericRecord
import org.apache.avro.SchemaBuilder
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

  def fromSpecCRecord(spec: CRecord): GenericRecord =
    new GenericRecordBuilder(CRecord.SCHEMA$)
      .set("id", spec.id)
      .set("cOptInt", spec.cOptInt.getOrElse(null))
      .set("cOptDouble", spec.cOptDouble.getOrElse(null))
      .set("bRecord", spec.bRecord.getOrElse(null))
      .set("ts", spec.ts.toEpochMilli)
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
      s.success.value shouldEqual b
    }
  }

  property("GenericToSpecific embedded Avro") {
    forAll { c: CRecord =>
      val v = fromSpecCRecord(c)
      val s = v.toSpecific(new CRecord())
      print(s)
      s.success.value shouldEqual c
    }
  }

  property("instanceOf property embedded avro") {
    val x = AvroUtils.instanceOf(classOf[CRecord])
    val y = new CRecord()
    x.id shouldEqual y.id
    x.cOptInt shouldEqual y.cOptInt
    x.cOptDouble shouldEqual y.cOptDouble
    x.bRecord shouldEqual y.bRecord
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
    val bw     = genOne[BWrapper]
    val g      = fromSpec(bw.$record)
    val config = new FlinkConfig(Array("test"))
    AvroUtils
      .toEmbeddedAvroInstance[BWrapper, BRecord, MyAvroADT](
        g,
        classOf[BRecord],
        config
      )
      .success
      .value shouldEqual bw

  }

  property("isGenericInstance property") {
    val b      = AvroUtils.instanceOf(classOf[BRecord])
    AvroUtils.isGenericInstance(b) shouldBe false
    val schema = SchemaBuilder
      .record("testrec")
      .fields()
      .requiredInt("int")
      .endRecord()
    val g      = new GenericRecordBuilder(schema).set("int", 17).build()
    AvroUtils.isGenericInstance(g) shouldBe true
  }

  property("isSpecificInstance property") {
    val b                       = AvroUtils.instanceOf(classOf[BRecord])
    AvroUtils.isSpecificInstance(b) shouldBe true
    def check(g: GenericRecord) =
      AvroUtils.isSpecificInstance(g)
    check(b) shouldBe true
    val schema                  = SchemaBuilder
      .record("testrec")
      .fields()
      .requiredInt("int")
      .endRecord()
    val g                       = new GenericRecordBuilder(schema).set("int", 17).build()
    check(g) shouldBe false
  }

}
