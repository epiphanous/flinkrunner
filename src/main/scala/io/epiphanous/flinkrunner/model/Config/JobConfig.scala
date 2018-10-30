package io.epiphanous.flinkrunner.model.Config
import com.typesafe.config.ConfigObject

case class JobConfig(
    name: String,
    description: String,
    help: String,
    sources: List[String],
    sinks: List[String],
    config: ConfigObject)
