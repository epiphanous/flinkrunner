package io.epiphanous.flinkrunner

import com.typesafe.config.ConfigRenderOptions
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink._
import io.epiphanous.flinkrunner.model.source._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils.AvroSchemaSerializer
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.data.RowData

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

/** FlinkRunner base class. All users of Flinkrunner will create their own
  * subclass. The only required parameter is a FlinkConfig object. Two
  * additional optional arguments exist for simplifying testing:
  *   - CheckResults - a class to provide inputs and check outputs to test
  *     your job's transformation functions
  *   - GeneratorFactory - a factory class to create DataGenerator
  *     instances to build random event streams for testing
  * @param config
  *   a flink runner configuration
  * @param checkResultsOpt
  *   an optional CheckResults class for testing
  * @param generatorFactoryOpt
  *   an optional GeneratorFactory instance to create data generators if
  *   you plan to use the GeneratorSource
  * @tparam ADT
  *   an algebraic data type for events processed by this flinkrunner
  */
abstract class FlinkRunner[ADT <: FlinkEvent: TypeInformation](
    val config: FlinkConfig,
    val checkResultsOpt: Option[CheckResults[ADT]] = None,
    val generatorFactoryOpt: Option[GeneratorFactory[ADT]] = None,
    val executeJob: Boolean = true)
    extends LazyLogging {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  env.getConfig.addDefaultKryoSerializer(
    classOf[Schema],
    classOf[AvroSchemaSerializer]
  )

  def getSourceOrSinkNames(sourceOrSink: String): Seq[String] =
    (config.getStringListOpt(s"$sourceOrSink.names") match {
      case sn if sn.nonEmpty => sn
      case _                 =>
        config
          .getObjectOption(s"${sourceOrSink}s")
          .map(
            _.unwrapped()
              .keySet()
              .asScala
              .toSeq
          )
          .getOrElse(Seq.empty)
    }).sorted

  val sourceNames: Seq[String] =
    getSourceOrSinkNames("source")

  val sinkNames: Seq[String] =
    getSourceOrSinkNames("sink")

  val sourceConfigs: Seq[SourceConfig[ADT]] =
    sourceNames.map(name =>
      SourceConfig[ADT](name, config, generatorFactoryOpt)
    )

  val sinkConfigs: Seq[SinkConfig[ADT]] =
    sinkNames.map(name => SinkConfig[ADT](name, config))

  val mainSinkConfigs: Seq[SinkConfig[ADT]] =
    sinkConfigs.filterNot(_.isSideOutput)

  val sideSinkConfigs: Seq[SinkConfig[ADT]] =
    sinkConfigs.filter(_.isSideOutput)

  def defaultSourceName: String = sourceConfigs
    .map(_.name)
    .headOption
    .getOrElse(
      throw new RuntimeException("no sources are configured")
    )

  def defaultSinkName: String = mainSinkConfigs
    .map(_.name)
    .headOption
    .getOrElse(
      throw new RuntimeException("no sinks are configured")
    )

  /** Gets (and returns as string) the execution plan for the job from the
    * StreamExecutionEnvironment.
    * @return
    *   String
    */
  def getExecutionPlan: String = env.getExecutionPlan

  /** Get the stream graph for the configured job. This is primarily useful
    * for testing the stream jobs constructed in flinkrunner. It will throw
    * an exception if you call it before running a job against this runner.
    * If you only are interested in the stream graph and don't need the job
    * to be executed, you can set executeJob = false when constructing the
    * FlinkRunner instance.
    * @return
    *   JobGraph
    */
  def getStreamGraph: StreamGraph = env.getStreamGraph(false)

  def getStreamNodesInfo: Seq[StreamNodeInfo] =
    StreamNodeInfo.from(getStreamGraph)

  /** Executes the job graph.
    * @return
    *   JobExecutionResult
    */
  def execute: JobExecutionResult = env.execute(config.jobName)

  config.showConfig match {
    case ShowConfigOption.None      => ()
    case ShowConfigOption.Concise   =>
      config._config.root().render(ConfigRenderOptions.concise())
    case ShowConfigOption.Formatted => config._config.root().render()
  }

  /** Invoke a job by name. Must be provided by an implementing class.
    *
    * @param jobName
    *   the job name
    */
  def invoke(jobName: String): Unit

  /** Invoke a job based on the job name and arguments passed in and handle
    * the result produced.
    */
  def process(): Unit = {
    if (config.jobName == "help") showHelp()
    else if (
      config.jobArgs.headOption
        .exists(s => List("help", "--help", "-help", "-h").contains(s))
    ) showJobHelp()
    else
      invoke(config.jobName)
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
          val desc = config
            .getStringOpt(s"jobs.$jn.description")
            .getOrElse("** no description **")
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
          |Try "${config.systemName} <jobName> --help" for details
          |${config.systemHelp}
      """.stripMargin
    error.foreach(m => logger.error(m))
    println(usage)
  }

  /** Helper method to resolve the source configuration. Implementers can
    * override this method to customize source configuration behavior, in
    * particular, the deserialization schemas used by flink runner.
    *
    * @param sourceName
    *   source name
    * @return
    *   SourceConfig
    */
  def getSourceConfig(
      sourceName: String = defaultSourceName): SourceConfig[ADT] =
    sourceConfigs
      .find(_.name.equalsIgnoreCase(sourceName))
      .getOrElse(
        throw new RuntimeException(
          s"unknown source <$sourceName> in job <${config.jobName}>"
        )
      )

  def _mockSource[E <: ADT: TypeInformation](
      sourceConfig: SourceConfig[ADT],
      mockEvents: Seq[E]): DataStream[E] = {
    val lbl = s"mock:${sourceConfig.label}"
    env.fromCollection(mockEvents).name(lbl).uid(lbl)
  }

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
    checkResultsOpt
      .map(c => c.getInputEvents[E](sourceConfig.name))
      .filter(_.nonEmpty)
      .fold(sourceConfig.getSourceStream[E](env))(
        _mockSource(sourceConfig, _)
      )

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
    *   EmbeddedAvroRecordFactory trait.
    * @tparam E
    *   stream element type (which must mixin EmbeddedAvroRecord)
    * @tparam A
    *   an avro record type (subclass of generic record)
    * @return
    *   DataStream[E]
    */
  def configToAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      sourceConfig: SourceConfig[ADT])(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] =
    checkResultsOpt
      .map(c => c.getInputEvents[E](sourceConfig.name))
      .filter(_.nonEmpty)
      .fold(sourceConfig.getAvroSourceStream[E, A](env))(
        _mockSource(sourceConfig, _)
      )

  /** Helper method to convert a source configuration into a DataStream[E]
    *
    * @param sourceConfig
    *   the source config
    * @param fromRowData
    *   an implicit method to convert a Row into an event of type E
    * @tparam E
    *   the event data type
    * @return
    */
  def configToRowSource[E <: ADT with EmbeddedRowType: TypeInformation](
      sourceConfig: SourceConfig[ADT])(implicit
      fromRowData: RowData => E): DataStream[E] = {
    checkResultsOpt
      .map(c => c.getInputEvents[E](sourceConfig.name))
      .filter(_.nonEmpty)
      .fold(sourceConfig.getRowSourceStream[E](env))(
        _mockSource(sourceConfig, _)
      )
  }

  //  ********************** SINKS **********************

  def getSinkConfig(
      sinkName: String = defaultSinkName): SinkConfig[ADT] = {
    sinkConfigs
      .find(_.name.equalsIgnoreCase(sinkName))
      .getOrElse(
        throw new RuntimeException(
          s"unknown sink <$sinkName> in job <${config.jobName}>"
        )
      )
  }

  /** Usually, we should write to the sink, unless we have a non-empty
    * CheckResults configuration that determines otherwise.
    *
    * @return
    *   true to write to the sink, false otherwise
    */
  def writeToSink: Boolean = checkResultsOpt.forall(_.writeToSink)

  def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkName: String): Unit =
    getSinkConfig(sinkName).addSink[E](stream)

  def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      stream: DataStream[E],
      sinkName: String): Unit =
    getSinkConfig(sinkName).addAvroSink[E, A](stream)

  def addRowSink[
      E <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag](
      stream: DataStream[E],
      sinkName: String = defaultSinkName): Unit =
    getSinkConfig(sinkName).addRowSink[E](stream)

}
