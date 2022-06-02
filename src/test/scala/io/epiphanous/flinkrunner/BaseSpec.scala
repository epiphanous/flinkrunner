package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.{AvroStreamJob, StreamJob}
import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.JobExecutionResult
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside, OptionValues, TryValues}

trait BaseSpec
    extends Matchers
    with OptionValues
    with EitherValues
    with TryValues
    with Inside
    with LazyLogging {

  def getRunner[ADT <: FlinkEvent](
      args: Array[String] = Array.empty,
      optConfig: Option[String] = None): FlinkRunner[ADT] = {
    val config = new FlinkConfig(args, optConfig)
    new FlinkRunner[ADT](config) {
      override def invoke(
          jobName: String): Either[List[ADT], JobExecutionResult] = ???
    }
  }

  /**
   * Create a test job using the [[StreamJob]] base class. The code
   * automatically arranges for the configuration's <tt>mock.edges</tt>
   * setting to be set to true, which is required to route the output of
   * the job to the test function <tt>mockSink</tt>.
   * @param args
   *   Simulated command line arguments for invoking the job.
   * @param configStr
   *   Simulated configuration, which together with <tt>args</tt> will be
   *   used to construct the flink runner's [[FlinkConfig]].
   * @param mockSources
   *   A map of collection based input arrays of type ADT.
   * @param mockSink
   *   The output stream of the job will be directed to this function,
   *   which takes a [[List]] [ADT] as input and has a [[Unit]] return
   *   type. You should include your tests of the job output in this
   *   function.
   * @param getJob
   *   A method to construct the job instance to be tested. This takes a
   *   string jobName as well as the constructed [[FlinkRunner]] object as
   *   arguments and must return an instance of the job to be tested.
   * @tparam JOB
   *   The type of the job instance, which in this case is a subclass of
   *   [[StreamJob]].
   * @tparam OUT
   *   The output type of the job, which must extend the ADT.
   * @tparam ADT
   *   The algebraic data type of the job.
   * @return
   *   An instance of [[FlinkRunner][[[ADT]] ]. You can run the job by
   *   invoking this instance's <tt>run</tt> method.
   */
  def getJobRunner[
      JOB <: StreamJob[OUT, ADT],
      OUT <: ADT,
      ADT <: FlinkEvent](
      args: Array[String],
      configStr: String,
      mockSources: Map[String, Seq[ADT]],
      mockSink: (List[ADT]) => Unit,
      getJob: (String, FlinkRunner[ADT]) => JOB): FlinkRunner[ADT] = {
    val config = new FlinkConfig(
      args,
      Some(configStr + "\nenvironment=dev\nmock.edges=true\n")
    )
    new FlinkRunner[ADT](config, mockSources, mockSink) {
      override def invoke(
          jobName: String): Either[List[ADT], JobExecutionResult] =
        getJob(jobName, this).run()
    }
  }

  /**
   * Create a test job using the [[AvroStreamJob]] base class. The code
   * automatically arranges for the configuration's <tt>mock.edges</tt>
   * setting to be set to true, which is required to route the output of
   * the job to the test function <tt>mockSink</tt>.
   *
   * Here's an example test job:
   * {{{
   *
   *   property("singleAvroSource property") {
   *     val cfg = """
   *       |sources {
   *       |  test-single {
   *       |  connector = collector
   *       |}
   *       |jobs {
   *       |  SingleAvroSourceJob {
   *       |    source = test
   *       |  }
   *       |}
   *       |""".stripMargin
   *     val src = Map("test-single" -> genPop[AWrapper](3))
   *     val getJob = (_: String, r: FlinkRunner[MyAvroADT]) => new
   *       SingleAvroSourceJob(r)
   *
   *     // this contains the tests
   *
   *     val mockSink = { r: List[MyAvroADT] =>
   *       val result = r.map(_.asInstanceOf[AWrapper]) val twoDays = 2 * 86400000
   *       val orig = src("test-single") result.head.$record.a0 shouldEqual
   *       orig.head.$record.a0 * 2 result.head.$record.a3 shouldEqual
   *       Instant.ofEpochMilli( orig.head.$record.a3.toEpochMilli + twoDays )
   *       println(orig.head)*
   *       println(result.head)
   *     }
   *
   *     // this creates and runs the job
   *     getAvroJobRunner[SingleAvroSourceJob, AWrapper, ARecord, MyAvroADT](
   *       Array("SingleAvroSourceJob"),
   *       cfg,
   *       src,
   *       mockSink,
   *       getJob
   *     ).process()
   * }
   *
   * }}}
   *
   * @param args
   *   Simulated command line arguments for invoking the job.
   * @param configStr
   *   Simulated configuration, which together with <tt>args</tt> will be
   *   used to construct the flink runner's [[FlinkConfig]].
   * @param mockSources
   *   A map of collection based input arrays of type ADT.
   * @param mockSink
   *   The output stream of the job will be directed to this function,
   *   which takes a [[List]] [ADT] as input and has a [[Unit]] return
   *   type. You should include your tests of the job output in this
   *   function.
   * @param getJob
   *   A method to construct the job instance to be tested. This takes a
   *   string jobName as well as the constructed [[FlinkRunner]] object as
   *   arguments and must return an instance of the job to be tested.
   * @tparam JOB
   *   The type of the job instance, which in this case is a subclass of
   *   [[AvroStreamJob]].
   * @tparam OUT
   *   The output type of the job, which must extend the [[ADT]] with
   *   [[EmbeddedAvroRecord]] of type [[A]].
   * @tparam A
   *   The avro record type for the job. This must be a subclass of an avro
   *   GenericRecord or SpecificRecord.
   * @tparam ADT
   *   The algebraic data type of the job.
   * @return
   *   An instance of <tt>FlinkRunner[ADT]</tt>. You can run the job by
   *   invoking this instance's <tt>run</tt> method.
   */
  def getAvroJobRunner[
      JOB <: AvroStreamJob[OUT, A, ADT],
      OUT <: ADT with EmbeddedAvroRecord[A],
      A <: GenericRecord,
      ADT <: FlinkEvent](
      args: Array[String],
      configStr: String,
      mockSources: Map[String, Seq[ADT]],
      mockSink: (List[ADT]) => Unit,
      getJob: (String, FlinkRunner[ADT]) => JOB): FlinkRunner[ADT] = {
    val config = new FlinkConfig(
      args,
      Some(configStr + "\nenvironment=dev\nmock.edges=true\n")
    )
    new FlinkRunner[ADT](config, mockSources, mockSink) {
      override def invoke(
          jobName: String): Either[List[ADT], JobExecutionResult] =
        getJob(jobName, this).run()
    }
  }

  lazy val nothingFlinkRunner: FlinkRunner[NothingADT] =
    getRunner[NothingADT]()

  lazy val mySimpleFlinkRunner: FlinkRunner[MySimpleADT] =
    getRunner[MySimpleADT]()
}
