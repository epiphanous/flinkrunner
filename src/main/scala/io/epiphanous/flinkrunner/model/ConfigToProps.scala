package io.epiphanous.flinkrunner.model
import java.util.Properties

import com.typesafe.config.ConfigObject

trait ConfigToProps {
  def config: Option[ConfigObject]
  val properties: Properties = {
    val p = new Properties()
    config match {
      case Some(c) => p.putAll(c.unwrapped)
      case None    =>
    }
    p
  }
}
