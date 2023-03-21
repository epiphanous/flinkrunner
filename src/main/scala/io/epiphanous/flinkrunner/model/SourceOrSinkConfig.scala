package io.epiphanous.flinkrunner.model

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps
import org.apache.flink.table.types.logical.utils.LogicalTypeParser
import org.apache.flink.table.types.logical.{LogicalTypeRoot, RowType}

import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties
import scala.util.Try

trait SourceOrSinkConfig extends LazyLogging {
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

  lazy val configuredRowType: Try[RowType] = Try(
    config
      .getString(pfx("row.type"))
  )
    .map(rt => if (rt.startsWith("ROW(")) rt else s"ROW($rt)")
    .map { rt =>
      val lt =
        LogicalTypeParser.parse(
          rt,
          Thread.currentThread.getContextClassLoader
        )
      if (lt.is(LogicalTypeRoot.ROW))
        lt.asInstanceOf[RowType]
      else
        throw new RuntimeException(
          s"row.type=$rt is an invalid flink logical ROW type definition"
        )
    }

  def notImplementedError(method: String): Unit =
    throw new RuntimeException(
      s"$method is not implemented for ${connector.entryName} ${_sourceOrSink} $name"
    )

}
