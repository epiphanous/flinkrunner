package io.epiphanous.flinkrunner.model.Config
import com.typesafe.config.ConfigObject

sealed trait SinkConfig {
  def name: String
}

case class KafkaSinkConfig(name: String, topic: String, config: ConfigObject) extends SinkConfig with ConfigToProps
case class KeyedKafkaSinkConfig(name: String, topic: String, config: ConfigObject) extends SinkConfig with ConfigToProps
case class KinesisSinkConfig(name: String, stream: String, config: ConfigObject) extends SinkConfig with ConfigToProps
case class FileSinkConfig(name: String, path: String) extends SinkConfig
case class SocketSinkConfig(name: String, serializerClass: String, host: String, port: Int) extends SinkConfig
case class JdbcSinkConfig(name: String, serializerClass: String, query: String, config: ConfigObject)
    extends SinkConfig
    with ConfigToProps
