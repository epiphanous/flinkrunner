package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.source.SourceConfig
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

import java.time.Instant

class AvroStreamJobSpec extends PropSpec {

  class MyAvroCheckResults extends CheckResults[MyAvroADT] {

    val inA: List[AWrapper] = genPop[AWrapper](3)
    val inB: List[BWrapper] = genPop[BWrapper](3)

    /** a name for this check configuration */
    override def name: String = "my-avro-check-results"

    /** Return a list of test events to run through a mock job.
      *
      * @tparam IN
      *   the input type
      * @return
      *   List[IN]
      */
    override def getInputEvents[IN <: MyAvroADT: TypeInformation](
        sourceConfig: SourceConfig[MyAvroADT]): List[IN] = {
      (sourceConfig.name match {
        case "test-arecord-kafka" => inA
        case "test-brecord-kafka" => inB
      }).asInstanceOf[List[IN]]
    }

    /** Check the results of a mock run of a job.
      *
      * @param out
      *   the list of output events
      * @tparam OUT
      *   the ourput event type
      */
    override def checkOutputEvents[OUT <: MyAvroADT: TypeInformation](
        out: List[OUT]): Unit = {
      implicitly[TypeInformation[OUT]].getTypeClass.getSimpleName match {
        case "AWrapper" => runTestA(inA, out.asInstanceOf[List[AWrapper]])
        case "BWrapper" => runTestB(inB, out.asInstanceOf[List[BWrapper]])
      }
    }

    def runTestA(in: List[AWrapper], out: List[AWrapper]): Unit = {
      val twoDays = 2 * 86400000
      in.zip(out).foreach { case (ina, outa) =>
        println(outa)
        println(ina)
        outa.$record.a0 shouldEqual (ina.$record.a0 * 2)
        outa.$record.a3 shouldEqual Instant.ofEpochMilli(
          ina.$record.a3.toEpochMilli + twoDays
        )
      }
    }

    def runTestB(in: List[BWrapper], out: List[BWrapper]): Unit = {
      println(out)
      println(in)
    }
  }

  property("singleAvroSource property") {
    val cfg    =
      """
        |sources {
        |  test-arecord-kafka {
        |    topic = arecords
        |    bootstrap.servers = "dogmo"
        |  }
        |  test-brecord-kafka {
        |    topic = brecords
        |    bootstrap.servers = "dogmo"
        |  }
        |}
        |jobs {
        |  SingleAvroSourceJob {
        |    source.names = test-arecord-kafka
        |  }
        |}
        |""".stripMargin
    val getJob =
      (_: String, r: FlinkRunner[MyAvroADT]) => new SingleAvroSourceJob(r)

    // this creates and runs the job
    getAvroJobRunner[SingleAvroSourceJob, AWrapper, ARecord, MyAvroADT](
      Array("SingleAvroSourceJob"),
      cfg,
      new MyAvroCheckResults(),
      getJob
    ).process()
  }

  property("connectedAvroSource property") {}

  property("filterByControlAvroSource property") {}

  property("broadcastConnectedAvroSource property") {}

}

class SingleAvroSourceJob(runner: FlinkRunner[MyAvroADT])(implicit
    fromKV: EmbeddedAvroRecordInfo[ARecord] => AWrapper)
    extends AvroStreamJob[AWrapper, ARecord, MyAvroADT](runner) {
  override def transform: DataStream[AWrapper] =
    singleAvroSource[AWrapper, ARecord]("test-arecord-kafka").map { a =>
      val (a0, a1, a2, a3) =
        (a.$record.a0, a.$record.a1, a.$record.a2, a.$record.a3)
      AWrapper(
        a.$record.copy(
          a0 + a0,
          2 * a1,
          2 * a2,
          Instant.ofEpochMilli(a3.toEpochMilli + 2 * 86400000)
        )
      )
    }
}
