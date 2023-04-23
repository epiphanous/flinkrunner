package io.epiphanous.flinkrunner.model

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps

import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties

trait SourceOrSinkConfig[ADT <: FlinkEvent] extends LazyLogging {
  def name: String

  def config: FlinkConfig

  def connector: FlinkConnectorName

  def _sourceOrSink: String

  def _sourceOrSinkPath: String = _sourceOrSink + "s"

  def pfx(path: String = ""): String = Seq(
    Some(_sourceOrSinkPath),
    Some(name),
    if (path.isEmpty) None else Some(path)
  ).flatten.mkString(".")

  val properties: Properties = config.getProperties(pfx("config"))

  lazy val propertiesMap: util.HashMap[String, String] =
    properties.asJavaMap

  lazy val label: String =
    s"${config.jobName.toLowerCase}/${connector.entryName.toLowerCase}/$name"

  lazy val stdUid: String = Hashing
    .sha256()
    .hashString(
      label,
      StandardCharsets.UTF_8
    )
    .toString

  lazy val uid: String = config.getStringOpt(pfx("uid")).getOrElse(stdUid)

  lazy val parallelism: Int = config
    .getIntOpt(pfx("parallelism"))
    .getOrElse(config.globalParallelism)

  def notImplementedError(method: String): Unit =
    throw new RuntimeException(
      s"$method is not implemented for ${connector.entryName} ${_sourceOrSink} $name"
    )

}
