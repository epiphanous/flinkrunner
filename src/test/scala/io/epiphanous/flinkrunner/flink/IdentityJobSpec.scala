package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.{MySimpleADT, SimpleA}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation

class IdentityJobSpec extends PropSpec {

  property("job works") {
    val input = genPop[SimpleA]()
    new FlinkRunner[MySimpleADT](
      getDefaultConfig,
      Some(
        getCheckResults[SimpleA, SimpleA, MySimpleADT](
          "job works",
          results => results shouldEqual input,
          input
        )
      )
    ) {
      override def invoke(jobName: String): Unit =
        new TableIdentityJob[SimpleA, MySimpleADT](this).run()
    }.process()
  }
}
