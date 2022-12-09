package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.flink.AvroStreamJob
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  MyAvroADT
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class SimpleAvroIdentityJob[
    E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation](runner: FlinkRunner[MyAvroADT])(
    implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends AvroStreamJob[E, A, MyAvroADT](runner) {

  override def transform: DataStream[E] = {
    singleAvroSource[E, A]().map { e: E =>
      println(e.$record.toString)
      e
    }
  }
}
