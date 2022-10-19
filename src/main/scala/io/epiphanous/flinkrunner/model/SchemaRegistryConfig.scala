package io.epiphanous.flinkrunner.model

import com.typesafe.config.ConfigObject
import io.epiphanous.flinkrunner.util.ConfigToProps.RichConfigObject
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps

import java.util
import scala.util.Try

case class SchemaRegistryConfig(
    isDeserializing: Boolean = false,
    url: String = "http://schema-registry:8082",
    cacheCapacity: Int = 1000,
    headers: util.HashMap[String, String] = new util.HashMap(),
    props: util.HashMap[String, String] = new util.HashMap()) {
  val isSerializing: Boolean = !isDeserializing
  props.put("schema.registry.url", url)
  props.put("specific.avro.reader", "false") // don't make this true!
  props.putIfAbsent("use.logical.type.converters", "true")
}
object SchemaRegistryConfig {
  def apply(
      isDeserializing: Boolean,
      configOpt: Option[ConfigObject]): SchemaRegistryConfig =
    configOpt
      .map { o =>
        val c             = o.toConfig
        val url           = c.getString("url")
        val cacheCapacity =
          Try(c.getInt("cache.capacity")).toOption.getOrElse(1000)
        val headers       =
          Try(c.getObject("headers")).toOption.asProperties.asJavaMap
        val props         =
          Try(c.getObject("props")).toOption.asProperties.asJavaMap
        SchemaRegistryConfig(
          isDeserializing = isDeserializing,
          url = url,
          cacheCapacity = cacheCapacity,
          headers = headers,
          props = props
        )
      }
      .getOrElse(SchemaRegistryConfig(isDeserializing))
}
