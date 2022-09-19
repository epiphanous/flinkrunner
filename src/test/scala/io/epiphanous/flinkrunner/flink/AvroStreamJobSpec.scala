package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.formats.parquet.avro.AvroParquetReaders
import org.apache.flink.streaming.api.scala._

import java.time.Instant

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
    val cfg    =
      """
        |sources {
        |  kafka_source {
        |    topic = bogus
        |    bootstrap.servers = "localhost:9092"
        |    bounded = true
        |  }
        |}
        |sinks {
        |  mock_sink {
        |  }
        |}
        |jobs {
        |  SingleAvroSourceJob {
        |  }
        |}
        |execution.runtime-mode = batch
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
  implicit val avroParquetRecordFormat: StreamFormat[ARecord] =
    AvroParquetReaders.forSpecificRecord(classOf[ARecord])
  override def transform: DataStream[AWrapper]                =
    singleAvroSource[AWrapper, ARecord]().map { a =>
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
