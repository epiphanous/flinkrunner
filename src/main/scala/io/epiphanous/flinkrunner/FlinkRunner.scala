package io.epiphanous.flinkrunner

import com.typesafe.config.ConfigRenderOptions
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink._
import io.epiphanous.flinkrunner.model.source._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.collection.JavaConverters._

/** Flink Job Invoker
  */
abstract class FlinkRunner[ADT <: FlinkEvent: TypeInformation](
    val config: FlinkConfig,
    val checkResultsOpt: Option[CheckResults[ADT]] = None)
    extends LazyLogging {

  /** the configured StreamExecutionEnvironment */
  val env: StreamExecutionEnvironment =
    config.configureStreamExecutionEnvironment

  /** the configured StreamTableEnvironment (for table jobs) */
  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  config.showConfig match {
    case ShowConfigOption.None      => ()
    case ShowConfigOption.Concise   =>
      config._config.root().render(ConfigRenderOptions.concise())
    case ShowConfigOption.Formatted => config._config.root().render()
  }

  /** Invoke a job by name. Must be provided by an implementing class.
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
    * @param sourceName
    *   source name
    * @return
    *   SourceConfig
    */
  def getSourceConfig(
      sourceName: String = getDefaultSourceName): SourceConfig[ADT] =
    SourceConfig[ADT](sourceName, config)

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
      sourceConfig: SourceConfig[ADT]): DataStream[E] = {
    checkResultsOpt
      .map(c => c.getInputEvents[E](sourceConfig.name))
      .getOrElse(List.empty[E]) match {
      case mockEvents if mockEvents.nonEmpty =>
        val lbl = s"mock:${sourceConfig.label}"
        env.fromCollection(mockEvents).name(lbl).uid(lbl)
      case _                                 =>
        sourceConfig match {
          case s: MockSourceConfig[ADT]     => s.getSourceStream[E](env)
          case s: FileSourceConfig[ADT]     => s.getSourceStream[E](env)
          case s: KafkaSourceConfig[ADT]    => s.getSourceStream[E](env)
          case s: KinesisSourceConfig[ADT]  => s.getSourceStream[E](env)
          case s: RabbitMQSourceConfig[ADT] => s.getSourceStream[E](env)
          case s: SocketSourceConfig[ADT]   => s.getSourceStream[E](env)
          case s: HybridSourceConfig[ADT]   => s.getSourceStream[E](env)
        }
    }
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
      .getOrElse(Seq.empty[E]) match {
      case mockEvents if mockEvents.nonEmpty =>
        val lbl = s"mock:${sourceConfig.label}"
        env.fromCollection(mockEvents).name(lbl).uid(lbl)
      case _                                 =>
        sourceConfig match {
          case s: FileSourceConfig[ADT]     => s.getAvroSourceStream[E, A](env)
          case s: KafkaSourceConfig[ADT]    =>
            s.getAvroSourceStream[E, A](env)
          case s: KinesisSourceConfig[ADT]  =>
            s.getAvroSourceStream[E, A](env)
          case s: RabbitMQSourceConfig[ADT] =>
            s.getAvroSourceStream[E, A](env)
          case s: SocketSourceConfig[ADT]   =>
            s.getAvroSourceStream[E, A](env)
          case s: HybridSourceConfig[ADT]   =>
            s.getAvroSourceStream[E, A](env)
        }
    }

  //  ********************** SINKS **********************

  /** Create a json-encoded stream sink from configuration.
    *
    * @param stream
    *   the data stream to send to sink
    * @param sinkName
    *   the sink to send it to
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
    * @param sinkName
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

  def getSinkConfig(
      sinkName: String = getDefaultSinkName): SinkConfig[ADT] =
    SinkConfig[ADT](sinkName, config)

  /** Usually, we should write to the sink, unless we have a non-empty
    * CheckResults configuration that determines otherwise.
    * @return
    *   true to write to the sink, false otherwise
    */
  def writeToSink: Boolean = checkResultsOpt.forall(_.writeToSink)

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
      case s: FileSinkConfig[ADT]  => s.getAvroSink[E, A](stream)
      case s                       =>
        throw new RuntimeException(
          s"Avro serialization not supported for ${s.connector} sinks"
        )
    }
}
