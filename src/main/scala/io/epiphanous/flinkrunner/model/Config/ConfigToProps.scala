package io.epiphanous.flinkrunner.model.Config
import java.util.Properties

import com.typesafe.config.ConfigObject

trait ConfigToProps {
  def config: ConfigObject
  val properties: Properties = {
    val p = new Properties()
    p.putAll(config.unwrapped)
    p
  }
}
