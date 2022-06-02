package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.{ARecord, AWrapper, MyAvroADT}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.streaming.api.scala._

import java.time.Instant

class AvroStreamJobSpec extends PropSpec {

  property("singleAvroSource property") {
    val cfg    =
      """
        |sources {
        |  test-single {
        |    connector = collector
        |  }
        |}
        |jobs {
        |  SingleAvroSourceJob {
        |    source = test
        |  }
        |}
        |""".stripMargin
    val src    = Map("test-single" -> genPop[AWrapper](3))
    val getJob =
      (_: String, r: FlinkRunner[MyAvroADT]) => new SingleAvroSourceJob(r)

    // this contains the tests
    val mockSink = { r: List[MyAvroADT] =>
      val result  = r.map(_.asInstanceOf[AWrapper])
      val twoDays = 2 * 86400000
      val orig    = src("test-single")
      result.head.$record.a0 shouldEqual orig.head.$record.a0 * 2
      result.head.$record.a3 shouldEqual Instant.ofEpochMilli(
        orig.head.$record.a3.toEpochMilli + twoDays
      )
      println(orig.head)
      println(result.head)
    }

    // this creates and runs the job
    getAvroJobRunner[SingleAvroSourceJob, AWrapper, ARecord, MyAvroADT](
      Array("SingleAvroSourceJob"),
      cfg,
      src,
      mockSink,
      getJob
    ).process()
  }

  property("connectedAvroSource property") {}

  property("filterByControlAvroSource property") {}

  property("broadcastConnectedAvroSource property") {}

}

class SingleAvroSourceJob(runner: FlinkRunner[MyAvroADT])(implicit
    fromKV: (Option[String], ARecord) => AWrapper)
    extends AvroStreamJob[AWrapper, ARecord, MyAvroADT](runner) {
  override def transform: DataStream[AWrapper] =
    singleAvroSource[AWrapper, ARecord]("test-single").map { a =>
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
