package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink._
import io.epiphanous.flinkrunner.model.source._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/** Flink Job Invoker
  */
abstract class FlinkRunner[ADT <: FlinkEvent: TypeInformation](
    val config: FlinkConfig,
    val mockSources: Map[String, Seq[ADT]] = Map.empty[String, Seq[ADT]],
    val mockSink: List[ADT] => Unit = { _: List[ADT] => () })
    extends LazyLogging {

  val env: StreamExecutionEnvironment  =
    config.configureStreamExecutionEnvironment
  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  /** Invoke a job by name. Must be provided by an implementing class. The
    * complex return type can be obtained by simply calling the run method
    * on an instance of a sub-class of FlinkRunner's StreamJob class.
    * @param jobName
    *   the job name
    * @return
    *   either a list of events output by the job or a
    *   [[JobExecutionResult]]
    */
  def invoke(jobName: String): Either[List[ADT], JobExecutionResult]

  /** Invoke a job based on the job name and arguments passed in and handle
    * the result produced.
    */
  def process(): Unit = {
    if (config.jobName == "help") showHelp()
    else if (
      config.jobArgs.headOption
        .exists(s => List("help", "--help", "-help", "-h").contains(s))
    ) showJobHelp()
    else {
      handleResults(invoke(config.jobName))
    }
  }

  /** Handles the complex return type of the invoke method, optionally
    * processing the list of events output by the job with the [[mockSink]]
    * function.
    * @param result
    *   the result of executing a streaming job
    */
  def handleResults(result: Either[List[ADT], JobExecutionResult]): Unit =
    result match {
      case Left(events)           => mockSink(events)
      case Right(executionResult) =>
        logger.debug(s"JOB DONE in ${executionResult.getNetRuntime}ms")
    }

  /** Show help for a particular job
    */
  def showJobHelp(): Unit = {
    val usage =
      s"""|${config.jobName} - ${config.jobDescription}
          |
          |Usage: ${config.systemName} ${config.jobName} [job parameters]
          |${config.jobHelp}
       """.stripMargin
    println(usage)
  }

  /** Show a help message regarding usage.
    *
    * @param error
    *   an optional error message to show
    */
  def showHelp(error: Option[String] = None): Unit = {
    val jobInfo = config.jobs.toList.sorted match {
      case s if s.isEmpty => "  *** No jobs defined ***"
      case s              =>
        s.map { jn =>
          val desc = config.getString(s"jobs.$jn.description")
          s"  - $jn: $desc"
        }.mkString("\n")
    }
    val usage   =
      s"""|
          |Usage: ${config.systemName} <jobName> [job parameters]
          |
          |Jobs:
          |
          |$jobInfo
          |
          |Try "${config.systemName} <jobName> --help" for details)
          |${config.systemHelp}
      """.stripMargin
    error.foreach(m => logger.error(m))
    println(usage)
  }

  /** Helper method to resolve the source configuration. Implementers can
    * override this method to customize source configuration behavior, in
    * particular, the deserialization schemas used by flink runner.
    * @param sourceName
    *   source name
    * @return
    *   SourceConfig
    */
  def getSourceConfig(sourceName: String): SourceConfig[ADT] =
    SourceConfig[ADT](sourceName, this)

  /** Helper method to convert a source config into a json-encoded source
    * data stream.
    *
    * @param sourceConfig
    *   the source config
    * @tparam E
    *   the stream event type
    * @return
    *   DataStream[E]
    */
  def configToSource[E <: ADT: TypeInformation](
      sourceConfig: SourceConfig[ADT]): DataStream[E] =
    sourceConfig match {
      case s: CollectionSourceConfig[ADT] =>
        s.getSource[E](env, mockSources)
      case s: FileSourceConfig[ADT]       => s.getSource[E](env)
      case s: KafkaSourceConfig[ADT]      => s.getSource[E](env)
      case s: KinesisSourceConfig[ADT]    => s.getSource[E](env)
      case s: RabbitMQSourceConfig[ADT]   => s.getSource[E](env)
      case s: SocketSourceConfig[ADT]     => s.getSource[E](env)
    }

  /** Helper method to convert a source config into an avro-encoded source
    * data stream. At the moment this is only supported for kafka sources
    * (and trivially for collection sources for testing).
    *
    * @param sourceConfig
    *   the source config
    * @param fromKV
    *   implicit function to construct a event from an optional key and
    *   avro value. This is usually provided by making sure the companion
    *   object of the stream element type mixes in the
    *   [[io.epiphanous.flinkrunner.model.EmbeddedAvroRecordFactory]]
    *   trait.
    * @tparam E
    *   stream element type (which must mixin [[EmbeddedAvroRecord]] )
    * @tparam A
    *   an avro record type (subclass of generic record)
    * @return
    *   DataStream[E]
    */
  def configToAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      sourceConfig: SourceConfig[ADT])(implicit
      fromKV: (Option[String], A) => E): DataStream[E] =
    sourceConfig match {
      case s: CollectionSourceConfig[ADT] => s.getSource(env, mockSources)
      case s: KafkaSourceConfig[ADT]      => s.getAvroSource[E, A](env)
      case s                              =>
        throw new RuntimeException(
          s"Avro deserialization not supported for ${s.connector} sources"
        )
    }

  //  ********************** SINKS **********************

  /** Create a json-encoded stream sink from configuration.
    *
    * @param stream
    *   the data stream to send to sink
    * @param sinkNameOpt
    *   an optional sink name to obtain configuration
    * @tparam E
    *   stream element type
    * @return
    *   DataStream[E]
    */
  def toSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkName: String
  ): Object =
    configToSink[E](stream, getSinkConfig(sinkName))

  /** Create an avro-encoded stream sink from configuration.
    * @param stream
    *   the data stream to send to the sink
    * @param sinkNameOpt
    *   an optional sink name (defaults to first sink)
    * @tparam E
    *   the event type
    * @tparam A
    *   the avro record type
    * @return
    *   the
    */
  def toAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      stream: DataStream[E],
      sinkName: String
  ): Object =
    configToAvroSink[E, A](stream, getSinkConfig(sinkName))

  def getSinkConfig(sinkName: String): SinkConfig[ADT] =
    SinkConfig[ADT](sinkName, config)

  def configToSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SinkConfig[ADT]): Object =
    sinkConfig match {
      case s: CassandraSinkConfig[ADT]     => s.getSink[E](stream)
      case s: ElasticsearchSinkConfig[ADT] => s.getSink[E](stream)
      case s: FileSinkConfig[ADT]          => s.getSink[E](stream)
      case s: JdbcSinkConfig[ADT]          => s.getSink[E](stream)
      case s: KafkaSinkConfig[ADT]         => s.getSink[E](stream)
      case s: KinesisSinkConfig[ADT]       => s.getSink[E](stream)
      case s: RabbitMQSinkConfig[ADT]      => s.getSink[E](stream)
      case s: SocketSinkConfig[ADT]        => s.getSink[E](stream)
    }

  def configToAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SinkConfig[ADT]): DataStreamSink[E] =
    sinkConfig match {
      case s: KafkaSinkConfig[ADT] => s.getAvroSink[E, A](stream)
      case s                       =>
        throw new RuntimeException(
          s"Avro serialization not supported for ${s.connector} sinks"
        )
    }
}
