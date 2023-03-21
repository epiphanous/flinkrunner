package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{IdentityMap, PropSpec}
import org.apache.flink.streaming.api.scala._

import java.time.Instant
import java.time.temporal.ChronoUnit

class AvroStreamJobSpec extends PropSpec {

  class MyAvroCheckResults extends CheckResults[MyAvroADT] {

    val inA: List[AWrapper] = genPop[AWrapper](3)

    /** a name for this check configuration */
    override val name: String = "my-avro-check-results"

    override val collectLimit: Int = inA.size

    /** Return a list of test events to run through a mock job.
      *
      * @tparam IN
      *   the input type
      * @return
      *   List[IN]
      */
    override def getInputEvents[IN <: MyAvroADT](name: String): List[IN] =
      inA.asInstanceOf[List[IN]]

    /** Check the results of a mock run of a job.
      *
      * @param out
      *   the list of output events
      * @tparam OUT
      *   the ourput event type
      */
    override def checkOutputEvents[OUT <: MyAvroADT](
        out: List[OUT]): Unit =
      runTestA(inA, out.asInstanceOf[List[AWrapper]])

    def runTestA(in: List[AWrapper], out: List[AWrapper]): Unit = {
//      logger.debug(
//        s"runTestA running with:${in.mkString("\nIN :", "\nIN :", "")}${out
//            .mkString("\nOUT:", "\nOUT:", "")}"
//      )
      out.size shouldEqual in.size
      val twoDays = 2 * 86400000
      in.zip(out).foreach { case (ina, outa) =>
        outa.$record.a0 shouldEqual (ina.$record.a0 * 2)
        outa.$record.a3 shouldEqual Instant.ofEpochMilli(
          ina.$record.a3.toEpochMilli + twoDays
        )
      }
    }

  }

  property("singleAvroSource property") {
    val cfg =
      """
        |sources {
        |  kafka_source {
        |    topic = bogus
        |    bootstrap.servers = "localhost:9092"
        |    bounded = true
        |  }
        |}
        |sinks {
        |  kafka_sink {
        |    topic = bogus2
        |    bootstrap.servers = "localhost:9092"
        |  }
        |}
        |jobs {
        |  SingleAvroSourceJob {
        |  }
        |}
        |execution.runtime-mode = batch
        |""".stripMargin

    // this creates and runs the job
    val checkResults = new MyAvroCheckResults()
    val times2       = new TimesTwo()
    getAvroStreamJobRunner[
      AWrapper,
      ARecord,
      AWrapper,
      ARecord,
      MyAvroADT
    ](cfg, times2, checkResults.inA, Some(checkResults))
      .process()
  }

  property("connectedAvroSource property") {}

  property("filterByControlAvroSource property") {}

  property("broadcastConnectedAvroSource property") {}

}

class TimesTwo() extends IdentityMap[AWrapper] {
  override def map(a: AWrapper): AWrapper =
    a.copy(
      a.$record.copy(
        a.$record.a0 * 2,
        a.$record.a1 * 2,
        a.$record.a2 * 2,
        a.$record.a3.plus(2, ChronoUnit.DAYS)
      )
    )
}
