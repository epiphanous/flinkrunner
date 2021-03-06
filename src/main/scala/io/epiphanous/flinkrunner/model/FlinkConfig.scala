package io.epiphanous.flinkrunner.model

import com.typesafe.config.{ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.{FlinkRunnerFactory, SEE}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.io.File
import java.time.Duration
import java.util.{Properties, List => JList, Map => JMap}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

@SerialVersionUID(1544548116L)
class FlinkConfig(
                   args: Array[String],
                   factory: FlinkRunnerFactory[_],
                   sources: Map[String, Seq[Array[Byte]]] = Map.empty,
                   optConfig: Option[String] = None)
  extends LazyLogging
    with Serializable {

  val (jobName, jobArgs, jobParams) = {
    val (n, a) = args match {
      case Array(opt, _*) if opt.startsWith("-") => ("help", args)
      case Array("help", _*) => ("help", args.tail)
      case Array(jn, "help", _*) => (jn, Array("--help") ++ args.tail)
      case Array(jn, _*) => (jn, args.tail)
      case _ => ("help", args)
    }
    (n, a, ParameterTool.fromArgs(a))
  }

  val _config = {
    val sc = Seq(ConfigFactory.load(), ConfigFactory.load("flink-runner.conf"))
    val ocf =
      if (jobParams.has("config"))
        Some(ConfigFactory.parseFile(new File(jobParams.get("config"))))
      else None
    val ocs = optConfig.map(ConfigFactory.parseString)
    // precedence in config is from right to left...
    (ocs ++ ocf ++ sc).foldRight(ConfigFactory.empty())((z, c) => z.withFallback(c))
  }

  def getCollectionSource(name: String) =
    sources.getOrElse(name, throw new RuntimeException(s"missing collection source $name"))

  val systemName = _config.getString("system.name")

  val jobs = _config.getObject("jobs").unwrapped().keySet().asScala.toSet

  def getJobConfig(name: String) = _config.getConfig(s"jobs.$name")

  private def _s(path: String): (String, String) = {
    val jpath = _j(path)
    if (jobParams.has(jpath)) ("a", jpath)
    else if (_config.hasPath(jpath)) ("c", jpath)
    else if (jobParams.has(path)) ("a", path)
    else ("c", path)
  }

  private def _j(path: String) = s"jobs.$jobName.$path"

  def getObject(path: String): ConfigObject = {
    val jpath = _j(path)
    if (_config.hasPath(jpath)) _config.getObject(jpath)
    else _config.getObject(path)
  }

  def getObjectOption(path: String): Option[ConfigObject] =
    Try(getObject(path)).toOption

  def getString(path: String): String =
    _s(path) match {
      case ("a", p) => jobParams.get(p)
      case (_, p) => _config.getString(p)
    }

  def getStringList(path: String): List[String] =
    _s(path) match {
      case ("a", p) => jobParams.get(p).split("[, ]+").toList
      case (_, p) => _config.getStringList(p).asScala.toList
    }

  def getInt(path: String): Int =
    _s(path) match {
      case ("a", p) => jobParams.getInt(p)
      case (_, p) => _config.getInt(p)
    }

  def getLong(path: String): Long =
    _s(path) match {
      case ("a", p) => jobParams.getLong(p)
      case (_, p) => _config.getLong(p)
    }

  def getBoolean(path: String): Boolean =
    _s(path) match {
      case ("a", p) => jobParams.getBoolean(p)
      case (_, p) => _config.getBoolean(p)
    }

  def getDouble(path: String): Double =
    _s(path) match {
      case ("a", p) => jobParams.getDouble(p)
      case (_, p) => _config.getDouble(p)
    }

  def getDuration(path: String): Duration =
    _s(path) match {
      case ("a", p) => ConfigFactory.parseString(s"$p = ${jobParams.get(p)}").getDuration(p)
      case (_, p) => _config.getDuration(p)
    }

  def getProperties(path: String): Properties = {
    val p = new Properties()

    def flatten(key: String, value: Object): Unit = {
      val pkey = if (key.isEmpty) key else s"$key."
      value match {
        case map: JMap[String, Object]@unchecked => map.asScala.foreach { case (k, v) => flatten(s"$pkey$k", v) }
        case list: JList[Object]@unchecked =>
          list.asScala.zipWithIndex.foreach { case (v, i) => flatten(s"$pkey$i", v) }
        case v =>
          p.put(key, v.toString)
          () // force unit return
      }
    }

    (_s(path) match {
      case ("a", p) => Some(ConfigFactory.parseString(s"$p = ${jobParams.get(p)}").getObject(p))
      case (_, p) => if (_config.hasPath(p)) Some(_config.getObject(p)) else None
    }) match {
      case Some(c) => flatten("", c.unwrapped())
      case None => // noop
    }
    p
  }

  def _classInstance[T](path: String): T =
    Class.forName(getString(path)).getDeclaredConstructor().newInstance().asInstanceOf[T]

  def getJobInstance = factory.getJobInstance(jobName)

  def getDeserializationSchema(sourceConfig: SourceConfig) = factory.getDeserializationSchema(sourceConfig)

  def getKafkaDeserializationSchema(sourceConfig: KafkaSourceConfig) =
    factory.getKafkaDeserializationSchema(sourceConfig)

  def getSerializationSchema(sinkConfig: SinkConfig) = factory.getSerializationSchema(sinkConfig)

  def getKafkaSerializationSchema(sinkConfig: KafkaSinkConfig) = factory.getKafkaSerializationSchema(sinkConfig)

  def getEncoder(sinkConfig: SinkConfig) = factory.getEncoder(sinkConfig)

  def getAddToJdbcBatchFunction(sinkConfig: SinkConfig) = factory.getAddToJdbcBatchFunction(sinkConfig)

  def getBucketAssigner(p: Properties) = factory.getBucketAssigner(p)

  def getSourceConfig(name: String): SourceConfig = SourceConfig(name, this)

  def getSinkConfig(name: String): SinkConfig = SinkConfig(name, this)

  def getSourceNames: Seq[String] =
    if (sources.nonEmpty) sources.keySet.toSeq
    else
      Try(getStringList("source.names")) match {
        case Success(sn) => sn
        case Failure(_) => getObject("sources").unwrapped().keySet().asScala.toSeq
      }

  def getSinkNames: Seq[String] =
    Try(getStringList("sink.names")) match {
      case Success(sn) => sn
      case Failure(_) => getObject("sinks").unwrapped().keySet().asScala.toSeq
    }

  lazy val environment = getString("environment")
  lazy val isDev = environment.startsWith("dev")
  lazy val isStage = environment.startsWith("stag")
  lazy val isProd = environment.startsWith("prod")

  def configureStreamExecutionEnvironment: SEE = {
    val env =
      if (isDev)
        StreamExecutionEnvironment.createLocalEnvironment(1)
      else
        StreamExecutionEnvironment.getExecutionEnvironment

    // use event time
    env.setStreamTimeCharacteristic(timeCharacteristic)

    // set parallelism
    env.setParallelism(globalParallelism)

    // configure check-pointing and state backend
    if (checkpointInterval > 0) {
      env.enableCheckpointing(checkpointInterval)

      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(checkpointMinPause.toMillis)

      env.getCheckpointConfig.setMaxConcurrentCheckpoints(checkpointMaxConcurrent)

      val backend = if (stateBackend == "rocksdb") {
        logger.info(s"Using ROCKS DB state backend at $checkpointUrl")
        val rocksBackend = new RocksDBStateBackend(checkpointUrl, checkpointIncremental)
        if (checkpointFlash)
          rocksBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
        rocksBackend
      } else {
        logger.info(s"Using FILE SYSTEM state backend at $checkpointUrl")
        new FsStateBackend(checkpointUrl)
      }
      /* this deprecation is annoying; its due to rocksdb's state backend
         extending AbstractStateBackend which is deprecated */
      env.setStateBackend(backend)
    }

    env
  }

  lazy val timeCharacteristic = {
    getString("time.characteristic").toLowerCase.replaceFirst("\\s*time$", "") match {
      case "event" => TimeCharacteristic.EventTime
      case "processing" => TimeCharacteristic.ProcessingTime
      case "ingestion" => TimeCharacteristic.IngestionTime
      case unknown => throw new RuntimeException(s"Unknown time.characteristic setting: '$unknown'")
    }
  }

  lazy val systemHelp = _config.getString("system.help")
  lazy val jobHelp = getString("help")
  lazy val jobDescription = getString("description")
  lazy val globalParallelism = getInt("global.parallelism")
  lazy val checkpointInterval = getLong("checkpoint.interval")
  lazy val checkpointMinPause = getDuration("checkpoint.min.pause")
  lazy val checkpointMaxConcurrent = getInt("checkpoint.max.concurrent")
  lazy val checkpointUrl = getString("checkpoint.url")
  lazy val checkpointFlash = getBoolean("checkpoint.flash")
  lazy val stateBackend = getString("state.backend").toLowerCase
  lazy val checkpointIncremental = getBoolean("checkpoint.incremental")
  lazy val showPlan = getBoolean("show.plan")
  lazy val mockEdges = isDev && getBoolean("mock.edges")
  lazy val maxLateness = getDuration("max.lateness")
  lazy val maxIdleness = getDuration("max.idleness")

}
