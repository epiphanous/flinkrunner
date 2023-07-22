package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.{ARecord, AWrapper, MyAvroADT}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation

class AvroIdentityJobSpec extends PropSpec {
  property("job works") {
    val input = genPop[AWrapper]()
    new FlinkRunner[MyAvroADT](
      getDefaultConfig,
      Some(
        getCheckResults[AWrapper, AWrapper, MyAvroADT](
          "job works",
          results => results shouldEqual input,
          input
        )
      )
    ) {
      override def invoke(jobName: String): Unit =
        new AvroIdentityJob[AWrapper, ARecord, MyAvroADT](this).run()
    }.process()
  }
}
