package io.epiphanous.flinkrunner.model

import com.typesafe.config.ConfigObject

import java.util.{Properties, List => JList, Map => JMap}
import scala.collection.JavaConverters._

object ConfigToProps {

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
