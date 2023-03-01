package io.epiphanous.flinkrunner.util

import com.typesafe.config.{ConfigException, ConfigObject}
import io.epiphanous.flinkrunner.model.FlinkConfig

import java.util.{Properties, List => JList, Map => JMap}
import scala.collection.JavaConverters._

object ConfigToProps {

  /** For the requested prefix p, ensure each property keys has a config
    * value that is the same at p.key and p.config.key.
    * @param config
    *   the FlinkConfig
    * @param p
    *   the prefix path
    * @param keys
    *   a list of property keys
    * @return
    *   properties (p.config as a Java Properties object)
    */
  def normalizeProps(
      config: FlinkConfig,
      p: String,
      keys: List[String]): Properties = {
    val props = config.getProperties(s"$p.config")
    keys.foreach { key =>
      val pkey  = s"$p.$key"
      val kBare = config.getStringOpt(pkey)
      val kProp = Option(props.getProperty(key))
      if (
        kBare.nonEmpty && kProp.nonEmpty && !kBare.get.equalsIgnoreCase(
          kProp.get
        )
      )
        throw new ConfigException.BadValue(
          pkey,
          s"$pkey and $p.config.$key are both specified but differ"
        )
      if (kBare.isEmpty && kProp.isEmpty)
        throw new ConfigException.Missing(pkey)
      if (kBare.nonEmpty && kProp.isEmpty)
        props.setProperty(key, kBare.get)
    }
    props
  }

  def getFromEither[T](
      p: String,
      names: Seq[String],
      getter: String => Option[T]
  ): Option[T] =
    names
      .flatMap(n => Seq(s"$p.$n", s"$p.config.$n"))
      .map(getter)
      .flatten
      .headOption

  implicit class RichConfigObject(val config: Option[ConfigObject]) {

    // this flattens a hierarchical config into a string -> string properties map
    def asProperties: Properties = {
      val p = new Properties()

      def flatten(key: String, value: Object): Unit = {
        val pkey = if (key.isEmpty) key else s"$key."
        value match {
          case map: JMap[String, Object] @unchecked =>
            map.asScala.foreach { case (k, v) => flatten(s"$pkey$k", v) }
          case list: JList[Object] @unchecked       =>
            list.asScala.zipWithIndex.foreach { case (v, i) =>
              flatten(s"$pkey$i", v)
            }
          case v                                    =>
            p.put(key, v.toString)
            () // force unit return
        }
      }

      config match {
        case Some(c) => flatten("", c.unwrapped())
        case None    => // noop
      }
      p
    }
  }
}
