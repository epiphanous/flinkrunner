package io.epiphanous.flinkrunner.model

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.ConfigToProps.RichConfigObject
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.io.File
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

@SerialVersionUID(1544548116L)
class FlinkConfig(
    args: Array[String],
    sources: Map[String, Seq[Array[Byte]]] = Map.empty,
    optConfig: Option[String] = None)
    extends LazyLogging
    with Serializable {

  val (jobName, jobArgs, jobParams) = {
    val (n, a) = args match {
      case Array(opt, _*) if opt.startsWith("-") => ("help", args)
      case Array("help", _*)                     => ("help", args.tail)
      case Array(jn, "help", _*)                 => (jn, Array("--help") ++ args.tail)
      case Array(jn, _*)                         => (jn, args.tail)
      case _                                     => ("help", args)
    }
    (n, a, ParameterTool.fromArgs(a))
  }

  val _config: Config = {
    val sc  =
      Seq(ConfigFactory.load(), ConfigFactory.load("flink-runner.conf"))
    val ocf =
      if (jobParams.has("config"))
        Some(ConfigFactory.parseFile(new File(jobParams.get("config"))))
      else None
    val ocs = optConfig.map(ConfigFactory.parseString)
    // precedence in config is from right to left...
    (ocs ++ ocf ++ sc).foldRight(ConfigFactory.empty())((z, c) =>
      z.withFallback(c)
    )
  }

  def getCollectionSource(name: String): Seq[Array[Byte]] =
    sources.getOrElse(
      name,
      throw new RuntimeException(s"missing collection source $name")
    )

  val systemName: String = _config.getString("system.name")

  val jobs: Set[String] =
    _config.getObject("jobs").unwrapped().keySet().asScala.toSet

  def getJobConfig(name: String): Config = _config.getConfig(s"jobs.$name")

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
      case (_, p)   => _config.getString(p)
    }

  def getStringOpt(path: String): Option[String] = Try(
    getString(path)
  ).toOption

  def getStringList(path: String): List[String] =
    _s(path) match {
      case ("a", p) => jobParams.get(p).split("[, ]+").toList
      case (_, p)   => _config.getStringList(p).asScala.toList
    }

  def getStringListOpt(path: String): List[String] =
    Try(getStringList(path)).getOrElse(List.empty[String])

  def getInt(path: String): Int =
    _s(path) match {
      case ("a", p) => jobParams.getInt(p)
      case (_, p)   => _config.getInt(p)
    }

  def getIntOpt(path: String): Option[Int] = Try(getInt(path)).toOption

  def getLong(path: String): Long =
    _s(path) match {
      case ("a", p) => jobParams.getLong(p)
      case (_, p)   => _config.getLong(p)
    }

  def getLongOpt(path: String): Option[Long] = Try(getLong(path)).toOption

  def getBoolean(path: String): Boolean =
    _s(path) match {
      case ("a", p) => jobParams.getBoolean(p)
      case (_, p)   => _config.getBoolean(p)
    }

  def getBooleanOpt(path: String): Option[Boolean] = Try(
    getBoolean(path)
  ).toOption

  def getDouble(path: String): Double =
    _s(path) match {
      case ("a", p) => jobParams.getDouble(p)
      case (_, p)   => _config.getDouble(p)
    }

  def getDoubleOpt(path: String): Option[Double] = Try(
    getDouble(path)
  ).toOption

  def getDuration(path: String): Duration =
    _s(path) match {
      case ("a", p) =>
        ConfigFactory
          .parseString(s"$p = ${jobParams.get(p)}")
          .getDuration(p)
      case (_, p)   => _config.getDuration(p)
    }

  def getDurationOpt(path: String): Option[Duration] = Try(
    getDuration(path)
  ).toOption

  def getProperties(path: String): Properties =
    (_s(path) match {
      case ("a", p) =>
        Some(
          ConfigFactory
            .parseString(s"$p = ${jobParams.get(p)}")
            .getObject(p)
        )
      case (_, p)   =>
        if (_config.hasPath(p)) Some(_config.getObject(p)) else None
    }).asProperties

  def _classInstance[T](path: String): T =
    Class
      .forName(getString(path))
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[T]

  def getSourceConfig(name: String): SourceConfig =
    SourceConfig(name, this)

  def getSinkConfig(name: String): SinkConfig = SinkConfig(name, this)

  def getSourceNames: Seq[String] =
    if (sources.nonEmpty) sources.keySet.toSeq
    else
      Try(getStringList("source.names")) match {
        case Success(sn) => sn
        case Failure(_)  =>
          getObject("sources").unwrapped().keySet().asScala.toSeq
      }

  def getSinkNames: Seq[String] =
    Try(getStringList("sink.names")) match {
      case Success(sn) => sn
      case Failure(_)  =>
        getObject("sinks").unwrapped().keySet().asScala.toSeq
    }

  lazy val environment: String =
    getStringOpt("environment").getOrElse("production")
  lazy val isDev: Boolean      = environment.startsWith("dev")
  lazy val isStage: Boolean    = environment.startsWith("stag")
  lazy val isProd: Boolean     = environment.startsWith("prod")

  def configureStreamExecutionEnvironment: StreamExecutionEnvironment = {
    val env =
      if (isDev)
        StreamExecutionEnvironment.createLocalEnvironment(1)
      else
        StreamExecutionEnvironment.getExecutionEnvironment

    // set parallelism
    env.setParallelism(globalParallelism)

    // configure check-pointing and state backend
    if (checkpointInterval > 0) {
      env.enableCheckpointing(checkpointInterval)

      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(
        checkpointMinPause.toMillis
      )

      env.getCheckpointConfig.setMaxConcurrentCheckpoints(
        checkpointMaxConcurrent
      )

      logger.info(s"Using ROCKS DB state backend at $checkpointUrl")
      env.setStateBackend(
        new EmbeddedRocksDBStateBackend(checkpointIncremental)
      )
      env.getCheckpointConfig.setCheckpointStorage(checkpointUrl)
    }

    env.setRuntimeMode(executionRuntimeMode)

    env
  }

  def getWatermarkStrategy(ws: String): String =
    ws.toLowerCase.replaceAll("[^a-z]", "") match {
      case "none"                  => "none"
      case "boundedlateness"       => "bounded lateness"
      case "boundedoutoforderness" => "bounded out of orderness"
      case "ascendingtimestamps"   => "ascending timestamps"
      case "monotonictimestamps"   => "ascending timestamps"
      case unknown                 =>
        throw new RuntimeException(
          s"Unknown watermark.strategy setting: '$unknown'"
        )
    }

  lazy val watermarkStrategy: String = getWatermarkStrategy(
    getString("watermark.strategy")
  )

  lazy val systemHelp: String                         = _config.getString("system.help")
  lazy val jobHelp: String                            = getString("help")
  lazy val jobDescription: String                     = getString("description")
  lazy val globalParallelism: Int                     = getInt("global.parallelism")
  lazy val checkpointInterval: Long                   = getLong("checkpoint.interval")
  lazy val checkpointMinPause: Duration               = getDuration(
    "checkpoint.min.pause"
  )
  lazy val checkpointMaxConcurrent: Int               = getInt(
    "checkpoint.max.concurrent"
  )
  lazy val checkpointUrl: String                      = getString("checkpoint.url")
  lazy val checkpointFlash: Boolean                   = getBoolean("checkpoint.flash")
  lazy val stateBackend: String                       = getString("state.backend").toLowerCase
  lazy val checkpointIncremental: Boolean             = getBoolean(
    "checkpoint.incremental"
  )
  lazy val showPlan: Boolean                          = getBoolean("show.plan")
  lazy val mockEdges: Boolean                         = isDev && getBoolean("mock.edges")
  lazy val maxLateness: Duration                      = getDuration("max.lateness")
  lazy val maxIdleness: Duration                      = getDuration("max.idleness")
  lazy val executionRuntimeMode: RuntimeExecutionMode =
    getStringOpt("execution.runtime-mode").map(_.toUpperCase) match {
      case Some("BATCH")     => RuntimeExecutionMode.BATCH
      case Some("AUTOMATIC") => RuntimeExecutionMode.AUTOMATIC
      case _                 => RuntimeExecutionMode.STREAMING
    }
}
