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
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils.AvroSchemaSerializer
import org.apache.flink.streaming.api.scala._
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
    val generatorFactoryOpt: Option[GeneratorFactory[ADT]] = None)
    extends LazyLogging {

  val env: StreamExecutionEnvironment =
    config.getStreamExecutionEnvironment

  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  env.getConfig.addDefaultKryoSerializer(
    classOf[Schema],
    classOf[AvroSchemaSerializer]
  )

  /** Gets (and returns as string) the execution plan for the job from the
    * StreamExecutionEnvironment.
    * @return
    *   String
    */
  def getExecutionPlan: String = env.getExecutionPlan

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

  def getSourceOrSinkNames(sourceOrSink: String): Seq[String] =
    (config.getStringListOpt(s"$sourceOrSink.names") match {
      case sn if sn.nonEmpty => sn
      case _                 =>
        config
          .getObject(s"${sourceOrSink}s")
          .unwrapped()
          .keySet()
          .asScala
          .toSeq
    }).sorted

  def getSourceNames: Seq[String] =
    getSourceOrSinkNames("source")

  def getSinkNames: Seq[String] =
    getSourceOrSinkNames("sink")

  def getDefaultSourceName: String = getSourceNames.headOption.getOrElse(
    throw new RuntimeException("no sources are configured")
  )

  def getDefaultSinkName: String = getSinkNames.headOption.getOrElse(
    throw new RuntimeException("no sinks are configured")
  )

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
      sourceName: String = getDefaultSourceName): SourceConfig[ADT] =
    SourceConfig[ADT](sourceName, config, generatorFactoryOpt)

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
    * @param fromRow
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
      sinkName: String = getDefaultSinkName): SinkConfig[ADT] =
    SinkConfig[ADT](sinkName, config)

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
    getSinkConfig(sinkName) match {
      case s: CassandraSinkConfig[ADT]     => s.addSink[E](stream)
      case s: ElasticsearchSinkConfig[ADT] => s.addSink[E](stream)
      case s: FileSinkConfig[ADT]          => s.addSink[E](stream)
      case s: JdbcSinkConfig[ADT]          => s.addSink[E](stream)
      case s: KafkaSinkConfig[ADT]         => s.addSink[E](stream)
      case s: KinesisSinkConfig[ADT]       => s.addSink[E](stream)
      case s: RabbitMQSinkConfig[ADT]      => s.addSink[E](stream)
      case s: SocketSinkConfig[ADT]        => s.addSink[E](stream)
      case s: IcebergSinkConfig[ADT]       => s.addSink[E](stream)
    }

  def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      stream: DataStream[E],
      sinkName: String): Unit =
    getSinkConfig(sinkName).addAvroSink[E, A](stream)

  def addRowSink[
      E <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag](
      stream: DataStream[E],
      sinkName: String = getDefaultSinkName): Unit =
    getSinkConfig(sinkName).addRowSink[E](stream)

}
