package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.TextLineDecoder
import io.epiphanous.flinkrunner.util.BoundedLatenessWatermarkStrategy
import io.epiphanous.flinkrunner.util.FileUtils.getResourceOrFile
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.{
  BulkWriter,
  Encoder,
  SerializationSchema
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.reader.{
  BulkFormat,
  StreamFormat
}
import org.apache.flink.connector.file.src.{FileSource, FileSourceSplit}
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcSink,
  JdbcStatementBuilder
}
import org.apache.flink.connector.kafka.sink.{
  KafkaRecordSerializationSchema,
  KafkaSink
}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{
  BasePathBucketAssigner,
  DateTimeBucketAssigner
}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{
  DefaultRollingPolicy,
  OnCheckpointRollingPolicy
}
import org.apache.flink.streaming.api.functions.sink.{
  SinkFunction,
  SocketClientSink
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType
import org.apache.flink.streaming.connectors.elasticsearch.{
  ElasticsearchSinkFunction,
  RequestIndexer
}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kinesis.serialization.{
  KinesisDeserializationSchema,
  KinesisSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.{
  FlinkKinesisConsumer,
  FlinkKinesisProducer
}
import org.apache.flink.streaming.connectors.rabbitmq.{
  RMQDeserializationSchema,
  RMQSink,
  RMQSinkPublishOptions,
  RMQSource
}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.net.URL
import java.time.Duration
import scala.collection.JavaConverters._

/**
 * Flink Job Invoker
 */
abstract class FlinkRunner[ADT <: FlinkEvent](
    val config: FlinkConfig,
    val mockSources: Map[String, Seq[ADT]] = Map.empty[String, Seq[ADT]],
    val mockSink: List[_] => Unit = (_: List[_]) => ())
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
  def invoke(jobName: String): Either[List[_], JobExecutionResult]

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
  def handleResults(result: Either[List[_], JobExecutionResult]): Unit =
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
   * Configure stream source from configuration.
   *
   * @param sourceNameOpt
   *   an optional name of the source to get its configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSource[E <: ADT: TypeInformation](
      sourceNameOpt: Option[String] = None
  ): DataStream[E] = {
    val name         = sourceNameOpt.getOrElse(
      config
        .getSourceNames(mockSources.keySet.toSeq)
        .headOption
        .getOrElse("events")
    )
    val sourceConfig = config.getSourceConfig(name)
    val uid          = sourceConfig.label
    val stream       = (sourceConfig match {
      case src: CollectionSourceConfig => fromCollection(src)
      case src: FileSourceConfig       => fromFile(src)
      case src: KafkaSourceConfig      => fromKafka(src)
      case src: KinesisSourceConfig    => fromKinesis(src)
      case src: RabbitMQSourceConfig   => fromRabbitMQ(src)
      case src: SocketSourceConfig     => fromSocket(src)
    }).name(uid).uid(uid)
    maybeAssignTimestampsAndWatermarks(stream, sourceConfig)
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
  def getCollectionSource[E <: ADT](name: String): Seq[E] =
    mockSources
      .getOrElse(
        name,
        throw new RuntimeException(s"missing collection source $name")
      )
      .filter(_.isInstanceOf[E])
      .map(_.asInstanceOf[E])

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
   * Get a stream format for a file source
   * @param sourceConfig
   *   the file source config
   * @tparam E
   *   the event type (ADT)
   * @return
   *   [[StreamFormat]] [E]
   */
  def getFileSourceStreamFormat[E <: ADT](
      sourceConfig: FileSourceConfig): StreamFormat[E] = ???

  /**
   * Get a bulk format for a file source
   * @param sourceConfig
   *   the file source config
   * @tparam E
   *   the event type (ADT)
   * @return
   *   [[BulkFormat]] [ E, [[FileSourceSplit]] ]
   */
  def getFileSourceBulkFormat[E <: ADT](
      sourceConfig: FileSourceConfig): BulkFormat[E, FileSourceSplit] = ???

  /**
   * Configure a file source
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
    val path    = new Path(getResourceOrFile(sourceConfig.path))
    val fsb     =
      if (sourceConfig.isBulk)
        FileSource.forBulkFileFormat(
          getFileSourceBulkFormat[E](sourceConfig),
          path
        )
      else
        FileSource
          .forRecordStreamFormat(
            getFileSourceStreamFormat[E](sourceConfig),
            path
          )
    val monitor = sourceConfig.propertiesMap
      .getOrDefault("monitor.continuously", "0")
      .toLong
    val fs      =
      if (monitor > 0)
        fsb.monitorContinuously(Duration.ofSeconds(monitor)).build()
      else fsb.build()
    env
      .fromSource(fs, WatermarkStrategy.noWatermarks(), sourceConfig.label)
      .name(s"raw:${sourceConfig.label}")
      .uid(s"raw:${sourceConfig.label}")
  }

  /**
   * Provide a record deserialization schema for a kafka source
   * @param sourceConfig
   *   kafka source config
   * @tparam E
   *   event (ADT) type
   * @return
   *   [[KafkaRecordDeserializationSchema]] [E]
   */
  def getKafkaRecordDeserializationSchema[E <: ADT](
      sourceConfig: KafkaSourceConfig)
      : KafkaRecordDeserializationSchema[E] = ???

  /**
   * Configure stream from kafka source.
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
  ): DataStream[E] = {
    logger.debug(s"sourceConfig = $sourceConfig")
    val ksb             = KafkaSource
      .builder[E]()
      .setTopics(sourceConfig.topic)
      .setProperties(sourceConfig.properties)
      .setStartingOffsets(sourceConfig.startingOffsets)
      .setDeserializer(
        getKafkaRecordDeserializationSchema[E](
          sourceConfig
        )
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
   * Provide a deserialization schema for a kinesis source
   * @param sourceConfig
   *   kinesis source config
   * @tparam E
   *   event (ADT) type
   * @return
   *   [[KinesisDeserializationSchema]] [E]
   */
  def getKinesisDeserializationSchema[E <: ADT](
      sourceConfig: KinesisSourceConfig): KinesisDeserializationSchema[E] =
    ???

  /**
   * Configure stream from kinesis.
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
        getKinesisDeserializationSchema[E](sourceConfig),
        sourceConfig.properties
      )
    )

  /**
   * Provide a deserialization schema for reading from a rabbit mq source
   * @param sourceConfig
   *   rabbit source configuration
   * @tparam E
   *   an ADT type
   * @return
   *   [[RMQDeserializationSchema]] [E]
   */
  def getRMQDeserializationSchema[E <: ADT](
      sourceConfig: RabbitMQSourceConfig): RMQDeserializationSchema[E] =
    ???

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
    val connConfig            = sourceConfig.connectionInfo.rmqConfig
    val deserializationSchema =
      getRMQDeserializationSchema[E](sourceConfig)
    env
      .addSource(
        new RMQSource(
          connConfig,
          sourceConfig.queue,
          sourceConfig.useCorrelationId,
          deserializationSchema
        )
      )
      .setParallelism(1) // required to get exactly once semantics
  }

  /**
   * Provide a deserialization schema for a socket source. The schema
   * receives the data from the socket as a utf-8 string and should return
   * an ADT type
   * @param sourceConfig
   *   socket source config
   * @tparam E
   *   event type (ADT)
   * @return
   *   [[DeserializationSchema]] [E]
   */
  def getSocketDeserializationSchema[E <: ADT](
      sourceConfig: SocketSourceConfig): TextLineDecoder[E] = ???

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
    val ds = getSocketDeserializationSchema(sourceConfig)
    env
      .socketTextStream(
        sourceConfig.host,
        sourceConfig.port
      )
      .name(s"raw:${sourceConfig.label}")
      .uid(s"raw:${sourceConfig.label}")
      .map(line => ds.decode(line))
  }

  val runner: FlinkRunner[ADT] = this

  implicit class EventStreamOps[E: TypeInformation](
      stream: DataStream[E]) {

    def as[T: TypeInformation]: DataStream[T] = {
      val name = stream.name
      stream
        .filter((e: E) => e.isInstanceOf[T @unchecked])
        .name(s"filter types $name")
        .uid(s"filter types $name")
        .map((e: E) => e.asInstanceOf[T @unchecked])
        .name(s"cast types $name")
        .uid(s"cast types $name")
    }

    def toSink(sinkNameOpt: Option[String] = None): Object =
      runner.toSink[E](stream, sinkNameOpt)

  }

  /**
   * Configure stream sink from configuration.
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
  def toSink[E: TypeInformation](
      stream: DataStream[E],
      sinkNameOpt: Option[String] = None
  ): Object = {
    val name       = sinkNameOpt.getOrElse(config.getSinkNames.head)
    val sinkConfig = config.getSinkConfig(name)
    val label      = sinkConfig.label
    sinkConfig match {
      case s: KafkaSinkConfig         =>
        stream
          .sinkTo(getKafkaSink(s))
          .uid(label)
          .name(label)
      case s: KinesisSinkConfig       =>
        stream
          .addSink(getKinesisSink[E](s))
          .uid(label)
          .name(label)
      case s: JdbcSinkConfig          =>
        stream.addSink(getJdbcSink[E](s)).uid(label).name(label)
      case s: FileSinkConfig          =>
        stream.sinkTo(getFileSink[E](s)).uid(label).name(label)
      case s: SocketSinkConfig        =>
        stream.addSink(getSocketSink[E](s)).uid(label).name(label)
      case s: ElasticsearchSinkConfig =>
        stream.addSink(getElasticsearchSink[E](s)).uid(label).name(label)
      case s: RabbitMQSinkConfig      =>
        stream.addSink(getRabbitMQSink[E](s)).uid(label).name(label)
      // annoyingly different api for cassandra sinks messes our api up a bit
      case s: CassandraSinkConfig     =>
        getCassandraSink[E](stream, s).uid(label).name(label)
    }
  }

  /**
   * Provide a record serialization schema for a kafka sink
   * @param sinkConfig
   *   kafka sink config
   * @tparam E
   *   event type
   * @return
   *   [[KafkaRecordSerializationSchema]] [E]
   */
  def getKafkaRecordSerializationSchema[E](
      sinkConfig: KafkaSinkConfig): KafkaRecordSerializationSchema[E] = ???

  /**
   * Create a kafka sink.
   *
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   KafkaSink[E]
   */
  def getKafkaSink[E: TypeInformation](
      sinkConfig: KafkaSinkConfig): KafkaSink[E] = {
    logger.debug(s"sinkConfig = $sinkConfig")
    KafkaSink
      .builder()
      .setBootstrapServers(sinkConfig.bootstrapServers)
      .setDeliverGuarantee(sinkConfig.deliveryGuarantee)
      .setTransactionalIdPrefix(sinkConfig.name)
      .setKafkaProducerConfig(sinkConfig.properties)
      .setRecordSerializer(
        getKafkaRecordSerializationSchema[E](
          sinkConfig
        )
      )
      .build()
  }

  /**
   * Provide a kinesis serialization schema for writing to a kinesis sink
   * @param sinkConfig
   *   kinesis sink config
   * @tparam E
   *   the event type
   * @return
   *   [[KinesisSerializationSchema]] [E]
   */
  def getKinesisSerializationSchema[E](
      sinkConfig: KinesisSinkConfig): KinesisSerializationSchema[E] = ???

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
  def getKinesisSink[E: TypeInformation](
      sinkConfig: KinesisSinkConfig): FlinkKinesisProducer[E] = {
    val sink =
      new FlinkKinesisProducer[E](
        getKinesisSerializationSchema(sinkConfig)
          .asInstanceOf[KinesisSerializationSchema[E]],
        sinkConfig.properties
      )
    sink.setDefaultStream(sinkConfig.stream)
    sink.setFailOnError(true)
    sink.setDefaultPartition("0")
    sink
  }

  /**
   * Return a jdbc statement builder to convert an event into a jdbc
   * statement
   * @param sinkConfig
   *   the jdbc sink config
   * @tparam E
   *   the event type
   * @return
   *   [[JdbcStatementBuilder]] [E]
   */
  def getJdbcSinkStatementBuilder[E](
      sinkConfig: JdbcSinkConfig): JdbcStatementBuilder[E] = ???

  /**
   * Create a jdbc sink.
   * @param sinkConfig
   *   a JdbcSinkConfig object
   * @tparam E
   *   the type of elements in the data stream
   * @return
   *   SinkFunction[E]
   */
  def getJdbcSink[E: TypeInformation](
      sinkConfig: JdbcSinkConfig
  ): SinkFunction[E] = {
    val sinkProps         = sinkConfig.properties
    val statementBuilder  = getJdbcSinkStatementBuilder[E](sinkConfig)
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
   * Get a row format file encoder
   * @param sinkConfig
   *   the streaming file sink config
   * @tparam E
   *   the event type to encode
   * @return
   *   [[Encoder]] [E]
   */
  def getFileSinkEncoder[E](sinkConfig: FileSinkConfig): Encoder[E] = ???

  /**
   * Get a bulk format file writer factory
   * @param sinkConfig
   *   the streaming file sink config
   * @tparam E
   *   the event type to encode
   * @return
   *   [[BulkWriter.Factory]] [E]
   */
  def getBulkFileWriter[E](
      sinkConfig: FileSinkConfig): BulkWriter.Factory[E] =
    ???

  /**
   * Provide a flink bucket assigner for writing to a streaming file sink
   * @param sinkConfig
   *   streaming file sink config
   * @tparam E
   *   event type
   * @return
   *   [[BucketAssigner]] [E, String]
   */
  def getBucketAssigner[E](
      sinkConfig: FileSinkConfig): BucketAssigner[E, String] =
    ???

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
  def getFileSink[E: TypeInformation](
      sinkConfig: FileSinkConfig
  ): FileSink[E] = {
    val props               = sinkConfig.properties
    val bucketCheckInterval =
      props.getProperty("bucket.check.interval", s"${60000}").toLong
    val bucketAssigner      =
      props.getProperty("bucket.assigner.type", "datetime") match {
        case "none"     => new BasePathBucketAssigner[E]()
        case "datetime" =>
          new DateTimeBucketAssigner[E](
            props.getProperty(
              "bucket.assigner.datetime.format",
              "YYYY/MM/DD/HH"
            )
          )
        case "custom"   => getBucketAssigner[E](sinkConfig)
        case other      =>
          throw new IllegalArgumentException(
            s"Unknown bucket assigner type '$other'."
          )
      }
    val path                = new Path(sinkConfig.path)
    sinkConfig.format match {
      case "row"  =>
        val rollingPolicy = DefaultRollingPolicy
          .builder()
          .withInactivityInterval(
            props
              .getProperty(
                "bucket.rolling.policy.inactivity.interval",
                s"${60000}"
              )
              .toLong
          )
          .withMaxPartSize(
            props
              .getProperty(
                "bucket.rolling.policy.max.part.size",
                s"${128 * 1024 * 1024}"
              )
              .toLong
          )
          .withRolloverInterval(
            props
              .getProperty(
                "bucket.rolling.policy.rollover.interval",
                s"${Long.MaxValue}"
              )
              .toLong
          )
          .build[E, String]()
        FileSink
          .forRowFormat(path, getFileSinkEncoder[E](sinkConfig))
          .withBucketAssigner(bucketAssigner)
          .withBucketCheckInterval(bucketCheckInterval)
          .withRollingPolicy(rollingPolicy)
          .build()
      case "bulk" =>
        val rollingPolicy = OnCheckpointRollingPolicy.build[E, String]()
        FileSink
          .forBulkFormat(path, getBulkFileWriter[E](sinkConfig))
          .withBucketAssigner(bucketAssigner)
          .withBucketCheckInterval(bucketCheckInterval)
          .withRollingPolicy(rollingPolicy)
          .build()
    }
  }

  /**
   * Provide a serialization schema for writing to a socket sink
   * @param sinkConfig
   *   a socket sink config
   * @tparam E
   *   event type
   * @return
   *   [[SerializationSchema]] [E]
   */
  def getSocketSerializationSchema[E](
      sinkConfig: SocketSinkConfig): SerializationSchema[E] = ???

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
      getSocketSerializationSchema[E](sinkConfig),
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
   * Return a function to index an event into an elasticsearch sink
   * @param sinkConfig
   *   the elasticsearch sink config
   * @tparam E
   *   the event type
   * @return
   *   [[ElasticsearchSinkFunction]] [E]
   */
  def getElasticsearchSinkEncoder[E](sinkConfig: ElasticsearchSinkConfig)
      : ElasticsearchSinkFunction[E] = {
    (element: E, _: RuntimeContext, indexer: RequestIndexer) =>
      val data = element.getClass.getDeclaredFields
        .filterNot(f => f.getName.startsWith("$"))
        .foldLeft(Map.empty[String, Any]) { case (a, f) =>
          f.setAccessible(true)
          val name = f.getName
          f.get(element) match {
            case Some(v: Any) => a + (name -> v)
            case None         => a
            case v: Any       => a + (name -> v)
          }
        }
        .asJava
      val req  = Requests.indexRequest(sinkConfig.index).source(data)
      indexer.add(req)
  }

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
  def getElasticsearchSink[E: TypeInformation](
      sinkConfig: ElasticsearchSinkConfig
  ): ElasticsearchSink[E] = {
    val hosts         = sinkConfig.transports.map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }.asJava
    val esSinkBuilder = new ElasticsearchSink.Builder[E](
      hosts,
      getElasticsearchSinkEncoder[E](sinkConfig)
    )

    val props = sinkConfig.properties
    Option(props.getProperty("bulk.flush.backoff.enable"))
      .map(_.toBoolean)
      .foreach(esSinkBuilder.setBulkFlushBackoff)
    Option(props.getProperty("bulk.flush.backoff.type"))
      .map(_.toUpperCase() match {
        case "CONSTANT"    => FlushBackoffType.CONSTANT
        case "EXPONENTIAL" => FlushBackoffType.EXPONENTIAL
        case t             =>
          logger.warn(
            s"invalid bulk.flush.backoff.type value '$t'; using CONSTANT"
          )
          FlushBackoffType.CONSTANT
      })
      .foreach(esSinkBuilder.setBulkFlushBackoffType)
    Option(
      props.getProperty("bulk.flush.backoff.delay")
    ).map(_.toLong)
      .foreach(esSinkBuilder.setBulkFlushBackoffDelay)
    Option(props.getProperty("bulk.flush.backoff.retries"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushBackoffRetries)
    Option(props.getProperty("bulk.flush.max.actions"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushMaxActions)
    Option(props.getProperty("bulk.flush.max.size.mb"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushMaxSizeMb)
    Option(props.getProperty("bulk.flush.interval.ms"))
      .map(_.toLong)
      .foreach(esSinkBuilder.setBulkFlushInterval)
    esSinkBuilder.build()
  }

  /**
   * Provide a deserialization schema for a socket source
   * @param sinkConfig
   *   rabbit sink config
   * @tparam E
   *   event type
   * @return
   *   [[DeserializationSchema]] [E]
   */
  def getRMQSerializationSchema[E](
      sinkConfig: RabbitMQSinkConfig): SerializationSchema[E] = ???

  /**
   * Provide an optional rabbit mq sink publish options object
   * @param sinkConfig
   *   rabbit sink configuration
   * @tparam E
   *   The event type
   * @return
   *   [[Option]] [ [[RMQSinkPublishOptions]] [E] ]
   */
  def getRMQSinkPublishOptions[E](
      sinkConfig: RabbitMQSinkConfig): Option[RMQSinkPublishOptions[E]] =
    None

  /**
   * Configure streaming to a rabbitmq sink.
   * @param sinkConfig
   *   a [[RabbitMQSinkConfig]] instance
   * @tparam E
   *   type of the flink runner ADT events in the stream
   * @return
   *   [[RMQSink]] [E]
   */
  def getRabbitMQSink[E: TypeInformation](
      sinkConfig: RabbitMQSinkConfig
  ): RMQSink[E] = {
    val name                = sinkConfig.name
    val connConfig          = sinkConfig.connectionInfo.rmqConfig
    val serializationSchema =
      getRMQSerializationSchema[E](sinkConfig)

    getRMQSinkPublishOptions[E](sinkConfig) match {
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
