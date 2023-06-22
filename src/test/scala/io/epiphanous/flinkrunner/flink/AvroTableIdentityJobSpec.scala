package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.{BRecord, BWrapper, MyAvroADT}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation

class AvroTableIdentityJobSpec extends PropSpec {

  property("job works") {
    val input = genPop[BWrapper]()
    new FlinkRunner[MyAvroADT](
      getDefaultConfig,
      Some(
        getCheckResults[BWrapper, BWrapper, MyAvroADT](
          "job works",
          results => results shouldEqual input,
          input
        )
      )
    ) {
      override def invoke(jobName: String): Unit =
        new AvroTableIdentityJob[BWrapper, BRecord, MyAvroADT](this).run()
    }.process()
  }

}
