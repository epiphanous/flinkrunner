package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.model.MySimpleADT
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class SimpleIdentityJob[E <: MySimpleADT: TypeInformation](
    runner: FlinkRunner[MySimpleADT])
    extends StreamJob[E, MySimpleADT](runner) {

  override def transform: DataStream[E] = {
    singleSource[E]().map { e: E =>
      println(e.toString)
      e
    }
  }
}
