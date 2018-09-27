package io.epiphanous.flinkrunner.flink

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._

class FlinkJobArgs(val jobName: String, val args: Array[String], val addedArgs: Set[FlinkArgDef]) extends LazyLogging {
  val params = ParameterTool.fromArgs(args)

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
    val connectors                = List("kafka", "jdbc", "cassandra")
    val defs                      = FlinkArgDef.CORE_FLINK_ARGUMENTS ++ addedArgs
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

  val missing = argDefs.filter(p => p.required && !params.has(p.name))

  def getString(name: String): String =
    if (isRequired(name)) params.getRequired(name) else params.get(name, getDefault(name))

  def getKafkaSources: List[String] = {
    getString("kafka.sources").split("[, ]+").toList
  }
  def getKafkaSinks: List[String] = {
    getString("kafka.sinks").split("[, ]+").toList
  }

  def getInt(name: String): Int =
    if (isRequired(name)) params.getRequired(name).toInt else params.getInt(name, getDefault(name, "0").toInt)
  def getLong(name: String): Long =
    if (isRequired(name)) params.getRequired(name).toLong else params.getLong(name, getDefault(name, "0").toLong)
  def getDouble(name: String): Double =
    if (isRequired(name)) params.getRequired(name).toDouble else params.getDouble(name, getDefault(name, "0").toDouble)
  def getBoolean(name: String): Boolean =
    if (isRequired(name)) params.getRequired(name).toBoolean
    else params.getBoolean(name, getDefault(name, "false").toBoolean)
  def getDefault(name: String, fallback: String = ""): String = getArgDef(name).flatMap(_.default).getOrElse(fallback)
  def isRequired(name: String): Boolean                       = getArgDef(name).exists(_.required)
  def getArgDef(name: String): Option[FlinkArgDef]            = argDefs.find(p => p.name.equalsIgnoreCase(name))
  def getProps(prefixes: List[String]) = {
    val properties = new Properties()
    val dottedPrefix = (prefixes.filter(_.nonEmpty) :+ "")
      .map(_.replaceAll("^\\.|\\.$", ""))
      .mkString(".")
    (
      params.getConfiguration
        .keySet()
        .asScala
        ++ FlinkArgDef.CORE_FLINK_ARGUMENTS.map(_.name)
    ).filter(_.startsWith(dottedPrefix))
      .foreach(key => {
        val name  = key.substring(dottedPrefix.length)
        val value = params.get(key, getDefault(key, ""))
        properties.setProperty(name, value)
      })
    properties
  }
  def isProd             = getString("environment").equalsIgnoreCase("shark")
  def isStage            = getString("environment").equalsIgnoreCase("frog")
  def isDev              = getString("environment").equalsIgnoreCase("dev")
  def mockEdges: Boolean = isDev && getBoolean("mock.edges")
  def showPlan: Boolean  = isDev && getBoolean("show.plan")
  def debug: Boolean     = getBoolean("debug")

}
