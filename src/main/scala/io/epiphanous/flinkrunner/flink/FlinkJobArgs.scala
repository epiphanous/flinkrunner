package io.epiphanous.flinkrunner.flink

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import enumeratum.EnumEntry.Snakecase
import enumeratum._
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

import scala.collection.JavaConverters._

class FlinkJobArgs(val jobName: String, val args: Array[String], val addedArgs: Set[FlinkArgDef]) extends LazyLogging {

  val params = {
    ParameterTool.fromArgs(args)
  }

  /**
    * Our predefined `source` and `sink` arg definitions need to be expanded
    * at runtime to handle the possibility that we need to configure multiple
    * source and sinks of a given type. For instance, we may need to read from
    * two `kafka` sources, or write to two `cassandra` sinks. For each kind of
    * connector we use, there is an arg definition to configure a list of
    * prefixes for that connector's sources and sinks. This code, expands the
    * set of argument definitions based on these configurations to make sure
    * we have the appropriate list of source and sink arg defs for all
    * connectors.
    *
    * For instance, if our job requires we read from two `kafka` topics, we
    * can specify `kafka.sources=src1,src2` and this will automatically add
    * argument definitions for all the configurations under
    * `src1.kafka.source.*` and `src2.kafka.source.*` and remove the
    * definitions for the default configurations under `kafka.source.*`.
    *
    */
  val argDefs = {
    val connectors = List("kinesis", "kafka", "jdbc", "cassandra", "file", "socket")
    val defs = FlinkArgDef.CORE_FLINK_ARGUMENTS ++ addedArgs
    def connectorDefs(ss: String) = defs.filter(_.name.startsWith(s"$ss."))
    val (add, remove) = connectors
      .map(connector => {
        val ea = List("sources", "sinks")
          .flatMap(s => {
            val pfx = s"$connector.$s"
            Option(params.get(pfx)) match {
              case Some(cs) if cs.length > 0 =>
                cs.split("[, ]+").map(prefix => connectorDefs(pfx).map(arg => arg.copy(name = s"$prefix.${arg.name}")))
              case _ => Set.empty
            }
          })
          .flatten
          .toSet
        (ea,
         if (ea.nonEmpty) connectorDefs(s"$connector.source") ++ connectorDefs(s"$connector.sink")
         else Set.empty[FlinkArgDef])
      })
      .fold((Set.empty[FlinkArgDef], Set.empty[FlinkArgDef])) { (z, a) =>
        (z._1 ++ a._1, z._2 ++ a._2)
      }
    defs ++ add -- remove
  }

  val missing = argDefs.filter(p => !params.has(p.name) && p.default.isEmpty)

  def getSinkPrefixes = {
    val prefixes = getString("sinks").split("[ ,]+").toList
    if (prefixes.nonEmpty) prefixes else List("")
  }

  def getSourcePrefixes = {
    val prefixes = getString("sources").split("[ ,]+").toList
    if (prefixes.nonEmpty) prefixes else List("")
  }

  def getSourceConfig[E <: FlinkEvent](
      prefix: String = "",
      sources: Map[String, Seq[Array[Byte]]]
    ): FlinkSourceConfig[E] = {
    val dottedPrefix = getDottedPrefix(List(prefix, "source"))
    val connectorString = getString("connector", dottedPrefix)
    val connector = FlinkConnectorName.withNameOption(connectorString) match {
      case Some(conn) => conn
      case _ => throw new IllegalArgumentException(s"Unsupported source connector: $connectorString")
    }
    val props = getProps(s"$dottedPrefix$connectorString")
    val deserializerClass = getString("deserializer.class", dottedPrefix)
    val deserializer = Class.forName(deserializerClass).newInstance().asInstanceOf[DeserializationSchema[E]]
    FlinkSourceConfig(connector, dottedPrefix, props, deserializer, sources)
  }

  def getSinkConfig[E <: FlinkEvent](prefix: String = ""): FlinkSinkConfig[E] = {
    val dottedPrefix = getDottedPrefix(List(prefix, "sink"))
    val connectorString = getString("connector", dottedPrefix)
    val connector = FlinkConnectorName.withNameOption(connectorString) match {
      case Some(conn) => conn
      case _ => throw new IllegalArgumentException(s"Unsupported sink connector: $connectorString")
    }
    val serializer = getClassInstanceOpt[SerializationSchema[E]]("serializer.class", dottedPrefix)
    val keyedSerializer = getClassInstanceOpt[KeyedSerializationSchema[E]]("keyed.serializer.class", dottedPrefix)
    val encoder = getClassInstanceOpt[Encoder[E]]("encoder.class", dottedPrefix)
    val jdbcBatchFunction = getClassInstanceOpt[AddToJdbcBatchFunction[E]]("batch.function.class", dottedPrefix)
    val props = getProps(s"$dottedPrefix$connectorString")
    FlinkSinkConfig(connector, dottedPrefix, props, serializer, keyedSerializer, encoder, jdbcBatchFunction)
  }

  def getParamOpt(name: String, prefix: String = ""): Option[String] = {
    val pname = s"$prefix$name"
    val paramOpt = Option(params.get(pname))
    if (paramOpt.nonEmpty) paramOpt else getDefaultOpt(pname)
  }

  def getParam(name: String, prefix: String = "") = {
    val pname = s"$prefix$name"
    if (isRequired(pname)) params.getRequired(pname) else params.get(pname, getDefault(pname))
  }

  def getString(name: String, prefix: String = ""): String = getParam(name, prefix)
  def getInt(name: String, prefix: String = ""): Int = getParam(name, prefix).toInt
  def getLong(name: String, prefix: String = ""): Long = getParam(name, prefix).toLong
  def getDouble(name: String, prefix: String = ""): Double = getParam(name, prefix).toDouble
  def getBoolean(name: String, prefix: String = ""): Boolean = getParam(name, prefix).toBoolean
  def getClassInstanceOpt[T](name: String, prefix: String = ""): Option[T] =
    getParamOpt(name, prefix).map(c => Class.forName(c).newInstance().asInstanceOf[T])

  def getDefaultOpt(name: String, prefix: String = ""): Option[String] = {
    val pname = s"$prefix$name"
    getArgDef(pname).flatMap(_.default)
  }

  def getDefault(name: String, prefix: String = ""): String = getDefaultOpt(name, prefix).get

  def isDefined(name: String, prefix: String = "") = getArgDef(name, prefix).nonEmpty

  def isRequired(name: String, prefix: String = ""): Boolean = {
    val pname = s"$prefix$name"
    getArgDef(pname).exists(_.default.isEmpty)
  }
  def getArgDef(name: String, prefix: String = ""): Option[FlinkArgDef] = {
    val pname = s"$prefix$name"
    argDefs.find(p => p.name.equalsIgnoreCase(pname))
  }
  def getProps(prefix: String): Properties = getProps(List(prefix))
  def getProps(prefixes: List[String]): Properties = {
    val properties = new Properties()
    val dottedPrefix = getDottedPrefix(prefixes)
    (
      params.getConfiguration
        .keySet()
        .asScala
        ++ FlinkArgDef.CORE_FLINK_ARGUMENTS.map(_.name)
    ).filter(_.startsWith(dottedPrefix))
      .foreach(key => {
        val name = key.substring(dottedPrefix.length)
        val value = params.get(key, getDefault(key))
        properties.setProperty(name, value)
      })
    properties
  }
  def getDottedPrefix(prefixes: List[String]) =
    (prefixes.filter(_.nonEmpty) :+ "")
      .map(_.replaceAll("^\\.|\\.$", ""))
      .mkString(".")

  def isProd: Boolean = getString("environment").equalsIgnoreCase("prod")
  def isStage: Boolean = getString("environment").equalsIgnoreCase("stage")
  def isDev: Boolean = getString("environment").equalsIgnoreCase("dev")
  def mockSink: Boolean = getBoolean("mock.sink")
  def showPlan: Boolean = isDev && getBoolean("show.plan")
  def debug: Boolean = getBoolean("debug")

}

sealed trait FlinkConnectorName extends EnumEntry with Snakecase
object FlinkConnectorName extends Enum[FlinkConnectorName] {
  val values = findValues
  case object Kinesis extends FlinkConnectorName
  case object Kafka extends FlinkConnectorName
  case object File extends FlinkConnectorName
  case object Socket extends FlinkConnectorName
  case object Cassandra extends FlinkConnectorName
  case object Jdbc extends FlinkConnectorName
  case object Collection extends FlinkConnectorName
}

sealed trait FlinkIOConfig {
  def connector: FlinkConnectorName
  def prefix: String
  def props: Properties

  def getString(key: String)(implicit args: Args): String =
    args.getString(key, prefix)
  def getInt(key: String)(implicit args: Args): Int =
    args.getInt(key, prefix)
  def getLong(key: String)(implicit args: Args): Long =
    args.getLong(key, prefix)
  def getDouble(key: String)(implicit args: Args): Double =
    args.getDouble(key, prefix)
  def getBoolean(key: String)(implicit args: Args): Boolean =
    args.getBoolean(key, prefix)

  def remove(key: String): Unit = props.remove(key)
}

case class FlinkSourceConfig[E <: FlinkEvent](
    connector: FlinkConnectorName,
    prefix: String,
    props: Properties,
    deserializer: DeserializationSchema[E],
    sources: Map[String, Seq[Array[Byte]]])
    extends FlinkIOConfig

case class FlinkSinkConfig[E <: FlinkEvent](
    connector: FlinkConnectorName,
    prefix: String,
    props: Properties,
    serializer: Option[SerializationSchema[E]] = None,
    keyedSerializer: Option[KeyedSerializationSchema[E]] = None,
    fileEncoder: Option[Encoder[E]] = None,
    jdbcBatchFunction: Option[AddToJdbcBatchFunction[E]] = None)
    extends FlinkIOConfig
