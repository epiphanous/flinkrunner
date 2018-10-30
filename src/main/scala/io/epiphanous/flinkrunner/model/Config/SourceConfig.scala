package io.epiphanous.flinkrunner.model.Config
import com.typesafe.config.ConfigObject

sealed trait SourceConfig {
  def name: String
}

case class KafkaSourceConfig(name: String, topic: String, config: ConfigObject) extends SourceConfig with ConfigToProps
case class KeyedKafkaSourceConfig(name: String, topic: String, config: ConfigObject)
    extends SourceConfig
    with ConfigToProps
case class KinesisSourceConfig(name: String, stream: String, config: ConfigObject)
    extends SourceConfig
    with ConfigToProps
case class FileSourceConfig(name: String, path: String) extends SourceConfig
case class SocketSourceConfig(name: String, host: String, port: Int) extends SourceConfig
