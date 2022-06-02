package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink._
import io.epiphanous.flinkrunner.model.source._
import io.epiphanous.flinkrunner.util.BoundedLatenessWatermarkStrategy
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.elasticsearch.sink.{
  Elasticsearch7SinkBuilder,
  ElasticsearchSink,
  FlushBackoffType
}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcSink
}
import org.apache.flink.connector.kafka.sink.{
  KafkaRecordSerializationSchema,
  KafkaSink
}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.{
  SinkFunction,
  SocketClientSink
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.rabbitmq.{RMQSink, RMQSource}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.http.HttpHost

import java.net.URL
import java.time.Duration

/**
 * Flink Job Invoker
 */
abstract class FlinkRunner[ADT <: FlinkEvent](
    val config: FlinkConfig,
    val mockSources: Map[String, Seq[ADT]] = Map.empty[String, Seq[ADT]],
    val mockSink: (List[ADT]) => Unit = { x: List[ADT] => () })
    extends LazyLogging {

  val env: StreamExecutionEnvironment  =
    config.configureStreamExecutionEnvironment
  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  /**
   * Invoke a job by name. Must be provided by an implementing class. The
   * complex return type can be obtained by simply calling the run method
   * on an instance of a sub-class of FlinkRunner's StreamJob class.
   * @param jobName
   *   the job name
   * @return
   *   either a list of events output by the job or a
   *   [[JobExecutionResult]]
   */
  def invoke(jobName: String): Either[List[ADT], JobExecutionResult]

  /**
   * Invoke a job based on the job name and arguments passed in and handle
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

  /**
   * Handles the complex return type of the invoke method, optionally
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

  /**
   * Show help for a particular job
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

  /**
   * Show a help message regarding usage.
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

  /**
   * Generates a timestamp and watermark assigner for a stream with a given
   * type of element that limits how late an element is allowed to arrive
   * in event time.
   *
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedLatenessWatermarks[E <: ADT: TypeInformation](
      streamID: String
  ) =
    new BoundedLatenessWatermarkStrategy[E](
      config.maxLateness,
      streamID
    )

  /**
   * Create a bounded of order watermark strategy with idleness checking
   *
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedOutOfOrderWatermarks[E <: ADT: TypeInformation]()
      : WatermarkStrategy[E] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(config.maxLateness)
      .withIdleness(config.maxIdleness)

  /**
   * Creates an ascending timestamp watermark strategy.
   * @tparam E
   *   type of stream element
   * @return
   *   AscendingTimestampExtractor[E]
   */
  def ascendingTimestampsWatermarks[E <: ADT: TypeInformation]()
      : WatermarkStrategy[E] = WatermarkStrategy.forMonotonousTimestamps()

  /**
   * Assign timestamps/watermarks if we're using event time
   * @param in
   *   the input stream to watermark
   * @tparam E
   *   event type
   * @return
   *   the possibly watermarked input stream
   */
  def maybeAssignTimestampsAndWatermarks[E <: ADT: TypeInformation](
      in: DataStream[E],
      sourceConfig: SourceConfig
  ): DataStream[E] =
    sourceConfig match {
      case _: KafkaSourceConfig => in
      case _                    =>
        in.assignTimestampsAndWatermarks(
          getWatermarkStrategy[E](sourceConfig)
        ).name(s"wm:${in.name}")
          .uid(s"wm:${in.name}")
    }

  /**
   * Return a watermark strategy based on the source configuration
   * @param sourceConfig
   *   a source configuration
   * @tparam E
   *   an ADT type
   * @return
   *   [[WatermarkStrategy]] [E]
   */
  def getWatermarkStrategy[E <: ADT: TypeInformation](
      sourceConfig: SourceConfig): WatermarkStrategy[E] = {
    sourceConfig.watermarkStrategy match {
      case "none"                 => WatermarkStrategy.noWatermarks[E]()
      case "bounded out of order" =>
        boundedOutOfOrderWatermarks[E]()
      case "ascending timestamps" => ascendingTimestampsWatermarks[E]()
      case _                      => boundedLatenessWatermarks[E](sourceConfig.name)
    }
  }

  /**
   * Configure a source stream from configuration.
   *
   * @param sourceNameOpt
   *   an optional name of the source to get its configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSource[E <: ADT: TypeInformation](
      sourceName: String
  ): DataStream[E] = {
    val sourceConfig = _resolveSourceConfig(sourceName)
    val uid          = sourceConfig.label
    val stream       = _sourceToStream[E](sourceConfig).name(uid).uid(uid)
    maybeAssignTimestampsAndWatermarks(stream, sourceConfig)
  }

  /**
   * Configure an avro-encoded source stream from configuration.
   *
   * @param sourceNameOpt
   *   an optional name of the source to get its configuration
   * @param fromKV
   *   implicit function to construct a event from an optional key and avro
   *   value
   * @tparam E
   *   stream element type
   * @tparam A
   *   an avro record type (subclass of container)
   * @return
   *   DataStream[E]
   */
  def fromAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](sourceName: String)(implicit
      fromKV: (Option[String], A) => E): DataStream[E] = {
    val sourceConfig = _resolveSourceConfig(sourceName)
    val uid          = sourceConfig.label
    val stream       = _avroSourceToStream[E, A](sourceConfig).name(uid).uid(uid)
    maybeAssignTimestampsAndWatermarks(stream, sourceConfig)
  }

  /**
   * Helper method to resolve the source configuration
   * @param sourceNameOpt
   *   optional source name (defaults to "events")
   * @return
   *   SourceConfig
   */
  def _resolveSourceConfig(sourceName: String): SourceConfig =
    config.getSourceConfig(sourceName)

  /**
   * Helper method to convert a source config into a json-encoded source
   * data stream.
   *
   * @param sourceConfig
   *   the source config
   * @tparam E
   *   the stream event type
   * @return
   *   DataStream[E]
   */
  def _sourceToStream[E <: ADT: TypeInformation](
      sourceConfig: SourceConfig): DataStream[E] = sourceConfig match {
    case src: CollectionSourceConfig => fromCollection[E](src)
    case src: FileSourceConfig       => fromFile[E](src)
    case src: KafkaSourceConfig      => fromKafka[E](src)
    case src: KinesisSourceConfig    => fromKinesis[E](src)
    case src: RabbitMQSourceConfig   => fromRabbitMQ[E](src)
    case src: SocketSourceConfig     => fromSocket[E](src)
  }

  /**
   * Helper method to convert a source config into an avro-encoded source
   * data stream. At the moment this is only supported for kafka sources.
   *
   * @param sourceConfig
   *   the source config
   * @param fromKV
   *   implicit function to construct a event from an optional key and avro
   *   value
   * @tparam E
   *   stream element type
   * @tparam A
   *   an avro record type (subclass of container)
   * @return
   *   DataStream[E]
   */
  def _avroSourceToStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](sourceConfig: SourceConfig)(
      implicit fromKV: (Option[String], A) => E): DataStream[E] =
    sourceConfig match {
      case src: CollectionSourceConfig => fromCollection(src)
      case src: KafkaSourceConfig      => fromAvroKafka[E, A](src)
      case src                         =>
        throw new RuntimeException(
          s"Avro deserialization not supported for ${src.connector} sources"
        )
    }

  /**
   * Utility method to get a collection source from configured mockSources
   * map
   * @param name
   *   of the source
   * @tparam E
   *   the expected adt type
   * @return
   *   a sequence of type E events
   */
  def getCollectionSource[E <: ADT](name: String): Seq[E] = {
    mockSources
      .getOrElse(
        name,
        throw new RuntimeException(s"missing collection source $name")
      )
      .map(_.asInstanceOf[E])
  }

  /**
   * Configure stream from collection source. This is used for testing job
   * logic and collections of adt instances should be provided in the
   * [[FlinkConfig]] [ADT] `sources` map.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type (ADT)
   * @return
   *   DataStream[E]
   */
  def fromCollection[E <: ADT: TypeInformation](
      sourceConfig: CollectionSourceConfig
  ): DataStream[E] =
    env
      .fromCollection(
        getCollectionSource[E](sourceConfig.topic)
      )
      .name(s"raw:${sourceConfig.label}")
      .uid(s"raw:${sourceConfig.label}")

  /**
   * Configure a file source.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type (ADT)
   * @return
   *   DataStream[E]
   */
  def fromFile[E <: ADT: TypeInformation](
      sourceConfig: FileSourceConfig
  ): DataStream[E] = {

    // all this grossness, because flink hasn't built a JsonStreamFormat?
    (if (sourceConfig.format == StreamFormatName.Json) {
       val rawName = s"raw:${sourceConfig.label}"
       val decoder = sourceConfig.getRowDecoder
       val fsb     =
         FileSource
           .forRecordStreamFormat[String](
             new TextLineInputFormat(),
             sourceConfig.destination
           )
       if (sourceConfig.monitorDuration > 0) {
         fsb.monitorContinuously(
           Duration.ofSeconds(sourceConfig.monitorDuration)
         )
       }
       env
         .fromSource(
           fsb.build(),
           WatermarkStrategy.noWatermarks(),
           rawName
         )
         .uid(rawName)
         .flatMap(line => decoder.decode(line).toOption)
         .name(sourceConfig.label)
     } else {
       // for all other source streams
       env.fromSource(
         sourceConfig.getStreamFileSource[E],
         WatermarkStrategy.noWatermarks(),
         sourceConfig.label
       )
     }).uid(sourceConfig.label)
  }

  /**
   * Configure stream from kafka source with json serialization.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKafka[E <: ADT: TypeInformation](
      sourceConfig: KafkaSourceConfig
  ): DataStream[E] =
    _fromKafka(sourceConfig, sourceConfig.getDeserializationSchema[E])

  /**
   * Configure stream from kafka source with avro serialization.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @tparam A
   *   avro record type
   * @return
   *   DataStream[E]
   */
  def fromAvroKafka[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      sourceConfig: KafkaSourceConfig
  )(implicit fromKV: (Option[String], A) => E): DataStream[E] =
    _fromKafka(
      sourceConfig,
      sourceConfig.getAvroDeserializationSchema[E, A]
    )

  /**
   * Helper method to generate kafka source stream
   * @param sourceConfig
   *   a kafka source config
   * @param deserializer
   *   the deserialization schema
   * @tparam E
   *   event type
   * @return
   *   DataStream[E]
   */
  def _fromKafka[E <: ADT: TypeInformation](
      sourceConfig: KafkaSourceConfig,
      deserializer: KafkaRecordDeserializationSchema[E]
  ): DataStream[E] = {
    val ksb             = KafkaSource
      .builder[E]()
      .setTopics(sourceConfig.topic)
      .setProperties(sourceConfig.properties)
      .setStartingOffsets(sourceConfig.startingOffsets)
      .setDeserializer(
        deserializer
      )
    val kafkaSrcBuilder =
      if (sourceConfig.bounded)
        ksb.setBounded(sourceConfig.stoppingOffsets)
      else ksb
    env
      .fromSource(
        kafkaSrcBuilder.build(),
        getWatermarkStrategy(sourceConfig),
        sourceConfig.label
      )
      .name(s"wm:${sourceConfig.label}")
      .uid(s"wm:${sourceConfig.label}")
  }

  /**
   * Configure stream from kinesis using json serialized messages.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKinesis[E <: ADT: TypeInformation](
      sourceConfig: KinesisSourceConfig
  ): DataStream[E] =
    env.addSource(
      new FlinkKinesisConsumer[E](
        sourceConfig.stream,
        sourceConfig.getDeserializationSchema,
        sourceConfig.properties
      )
    )

  /**
   * Configure a stream from rabbitmq.
   * @param sourceConfig
   *   a RabbitMQSourceConfig instance
   * @tparam E
   *   instance type of flink runner ADT
   * @return
   *   DataStream[E]
   */
  def fromRabbitMQ[E <: ADT: TypeInformation](
      sourceConfig: RabbitMQSourceConfig): DataStream[E] = {
    val connConfig = sourceConfig.connectionInfo.rmqConfig
    env
      .addSource(
        new RMQSource(
          connConfig,
          sourceConfig.queue,
          sourceConfig.useCorrelationId,
          sourceConfig.getDeserializationSchema[E]
        )
      )
      .setParallelism(1) // required to get exactly once semantics
  }

  /**
   * Configure stream from socket source. This requires a deserialization
   * schema to be provided that will get each line of text from the socket
   * as a byte array, and must convert it into an ADT type.
   *
   * @param sourceConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSocket[E <: ADT: TypeInformation](
      sourceConfig: SocketSourceConfig
  ): DataStream[E] = {
    val decoder = sourceConfig.getRowDecoder[E]
    env
      .socketTextStream(
        sourceConfig.host,
        sourceConfig.port
      )
      .name(s"raw:${sourceConfig.label}")
      .uid(s"raw:${sourceConfig.label}")
      .flatMap(line => decoder.decode(line).toOption)
  }

  /**
   * ************************ SINKS ************************
   */

  /**
   * Create a json-encoded stream sink from configuration.
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
      sinkNameOpt: Option[String] = None
  ): Object =
    _configToSink[E](stream, _resolveSinkConfig(sinkNameOpt))

  /**
   * Create an avro-encoded stream sink from configuration.
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
      sinkNameOpt: Option[String] = None
  ): Object =
    _configToAvroSink[E, A](stream, _resolveSinkConfig(sinkNameOpt))

  def _resolveSinkConfig(sinkNameOpt: Option[String]): SinkConfig =
    config.getSinkConfig(sinkNameOpt.getOrElse(config.getSinkNames.head))

  def _configToSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SinkConfig): Object = {
    val label = sinkConfig.label
    sinkConfig match {
      case s: KafkaSinkConfig         =>
        stream
          .sinkTo(getKafkaSink[E](s))
          .uid(label)
          .name(label)
      case s: KinesisSinkConfig       =>
        stream
          .sinkTo(getKinesisSink[E](s))
          .uid(label)
          .name(label)
      case s: JdbcSinkConfig          =>
        stream.addSink(getJdbcSink[E](s)).uid(label).name(label)
      case s: FileSinkConfig          =>
        stream.sinkTo(getFileSink[E](s)).uid(label).name(label)
      case s: SocketSinkConfig        =>
        stream.addSink(getSocketSink[E](s)).uid(label).name(label)
      case s: ElasticsearchSinkConfig =>
        stream.sinkTo(getElasticsearchSink[E](s)).uid(label).name(label)
      case s: RabbitMQSinkConfig      =>
        stream.addSink(getRabbitMQSink[E](s)).uid(label).name(label)
      // annoyingly different api for cassandra sinks messes our api up a bit
      case s: CassandraSinkConfig     =>
        getCassandraSink[E](stream, s).uid(label).name(label)
    }
  }

  def _configToAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SinkConfig): DataStreamSink[E] = {
    val label = sinkConfig.label
    sinkConfig match {
      case s: KafkaSinkConfig =>
        stream
          .sinkTo(getKafkaAvroSink[E, A](s))
          .uid(label)
          .name(label)
      case s                  =>
        throw new RuntimeException(
          s"Avro serialization not supported for ${s.connector} sinks"
        )
    }
  }

  /**
   * Create a json-encoded kafka sink.
   *
   * @param sinkConfig
   *   a kafka sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   KafkaSink[E]
   */
  def getKafkaSink[E <: ADT: TypeInformation](
      sinkConfig: KafkaSinkConfig): KafkaSink[E] =
    _getKafkaSink(sinkConfig, sinkConfig.getSerializationSchema[E])

  /**
   * Create an avro-encoded kafka sink.
   *
   * @param sinkConfig
   *   a kafka sink config
   * @tparam E
   *   stream element type
   * @tparam A
   *   avro record type
   * @return
   *   KafkaSink[E]
   */
  def getKafkaAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord](sinkConfig: KafkaSinkConfig): KafkaSink[E] =
    _getKafkaSink(sinkConfig, sinkConfig.getAvroSerializationSchema[E, A])

  /**
   * Helper method to create a kafka sink with a provided serializer.
   * @param sinkConfig
   *   kafka sink configuration
   * @param serializer
   *   the serializer
   * @tparam E
   *   stream event type
   * @return
   *   KafkaSink[E]
   */
  def _getKafkaSink[E <: ADT: TypeInformation](
      sinkConfig: KafkaSinkConfig,
      serializer: KafkaRecordSerializationSchema[E]): KafkaSink[E] =
    KafkaSink
      .builder()
      .setBootstrapServers(sinkConfig.bootstrapServers)
      .setDeliverGuarantee(sinkConfig.deliveryGuarantee)
      .setTransactionalIdPrefix(sinkConfig.name)
      .setKafkaProducerConfig(sinkConfig.properties)
      .setRecordSerializer(serializer)
      .build()

  /**
   * Create a kinesis sink.
   *
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   FlinkKinesisProducer[E]
   */
  def getKinesisSink[E <: ADT: TypeInformation](
      sinkConfig: KinesisSinkConfig): KinesisStreamsSink[E] =
    KinesisStreamsSink
      .builder[E]
      .setStreamName(sinkConfig.stream)
      .setFailOnError(true)
      .setSerializationSchema(sinkConfig.getSerializationSchema[E])
      .setKinesisClientProperties(sinkConfig.properties)
      .build()

  /**
   * Create a jdbc sink.
   * @param sinkConfig
   *   a JdbcSinkConfig object
   * @tparam E
   *   the type of elements in the data stream
   * @return
   *   SinkFunction[E]
   */
  def getJdbcSink[E <: ADT: TypeInformation](
      sinkConfig: JdbcSinkConfig
  ): SinkFunction[E] = {
    val sinkProps         = sinkConfig.properties
    val statementBuilder  = sinkConfig.getStatementBuilder[E]
    val executionOptions  = JdbcExecutionOptions
      .builder()
      .withMaxRetries(
        Option(sinkProps.getProperty("max.retries"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES)
      )
      .withBatchSize(
        Option(sinkProps.getProperty("batch.size"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_SIZE)
      )
      .withBatchIntervalMs(
        Option(sinkProps.getProperty("batch.interval.ms"))
          .map(o => o.toLong)
          .getOrElse(60L)
      )
      .build()
    val connectionOptions =
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(sinkConfig.url)
        .build()
    JdbcSink.sink(
      sinkConfig.query,
      statementBuilder,
      executionOptions,
      connectionOptions
    )
  }

  /**
   * Get a row-formatted file sink. Implementor must provide an encoder to
   * convert the adt instance into a row to write to the file (ie, json or
   * csv).
   *
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   [[FileSink]] [E]
   */
  def getFileSink[E <: ADT: TypeInformation](
      sinkConfig: FileSinkConfig
  ): FileSink[E] = sinkConfig.getFileSink[E]

  /**
   * Send stream to a socket sink.
   *
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def getSocketSink[E: TypeInformation](
      sinkConfig: SocketSinkConfig
  ) =
    new SocketClientSink[E](
      sinkConfig.host,
      sinkConfig.port,
      sinkConfig.getSerializationSchema[E],
      sinkConfig.maxRetries.getOrElse(0),
      sinkConfig.autoFlush.getOrElse(false)
    )

  /**
   * Get a cassandra sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def getCassandraSink[E: TypeInformation](
      stream: DataStream[E],
      sinkConfig: CassandraSinkConfig): CassandraSink[E] =
    CassandraSink
      .addSink(stream)
      .setHost(sinkConfig.host)
      .setQuery(sinkConfig.query)
      .build()

  /**
   * Get an elasticsearch sink.
   *
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def getElasticsearchSink[E <: ADT: TypeInformation](
      sinkConfig: ElasticsearchSinkConfig
  ): ElasticsearchSink[E] = {
    val hosts = sinkConfig.transports.map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }
    val props = sinkConfig.properties
    val esb   =
      new Elasticsearch7SinkBuilder[E]
        .setHosts(hosts: _*)
        .setEmitter[E](sinkConfig.getEmitter[E])
        .setBulkFlushBackoffStrategy(
          FlushBackoffType
            .valueOf(props.getProperty("bulk.flush.backoff.type", "NONE")),
          props.getProperty("bulk.flush.backoff.retries", "5").toInt,
          props.getProperty("bulk.flush.backoff.delay", "1000").toLong
        )
    Option(props.getProperty("bulk.flush.max.actions"))
      .map(_.toInt)
      .foreach(esb.setBulkFlushMaxActions)
    Option(props.getProperty("bulk.flush.max.size.mb"))
      .map(_.toInt)
      .foreach(esb.setBulkFlushMaxSizeMb)
    Option(props.getProperty("bulk.flush.interval.ms"))
      .map(_.toLong)
      .foreach(esb.setBulkFlushInterval)
    esb.build()
  }

  /**
   * Configure streaming to a rabbitmq sink.
   * @param sinkConfig
   *   a [[RabbitMQSinkConfig]] instance
   * @tparam E
   *   type of the flink runner ADT events in the stream
   * @return
   *   [[RMQSink]] [E]
   */
  def getRabbitMQSink[E <: ADT: TypeInformation](
      sinkConfig: RabbitMQSinkConfig
  ): RMQSink[E] = {
    val name                = sinkConfig.name
    val connConfig          = sinkConfig.connectionInfo.rmqConfig
    val serializationSchema =
      sinkConfig.getSerializationSchema[E]

    sinkConfig.getPublishOptions[E] match {
      case Some(p) => new RMQSink(connConfig, serializationSchema, p)
      case None    =>
        sinkConfig.queue match {
          case Some(q) => new RMQSink(connConfig, q, serializationSchema)
          case None    =>
            throw new RuntimeException(
              s"RabbitMQ config requires either a queue name or publishing options for sink $name"
            )
        }
    }
  }

}
