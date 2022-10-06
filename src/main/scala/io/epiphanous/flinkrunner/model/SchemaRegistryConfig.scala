package io.epiphanous.flinkrunner.model

import com.typesafe.config.ConfigObject
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaRegistryClient
}
import io.epiphanous.flinkrunner.util.ConfigToProps.RichConfigObject
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps

import java.util
import scala.util.Try

case class SchemaRegistryConfig(
    url: String = "http://schema-registry:8082",
    cacheCapacity: Int = 1000,
    headers: util.HashMap[String, String] = new util.HashMap(),
    props: util.HashMap[String, String] = new util.HashMap()) {
  props.put("schema.registry.url", url)
  props.putIfAbsent("use.logical.type.converters", "true")
  props.putIfAbsent("specific.avro.reader", "true")
  def getClient: SchemaRegistryClient = {
    new CachedSchemaRegistryClient(
      url,
      cacheCapacity,
      props,
      headers
    )
  }
}
object SchemaRegistryConfig {
  def apply(configOpt: Option[ConfigObject]): SchemaRegistryConfig =
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
          url = url,
          cacheCapacity = cacheCapacity,
          headers = headers,
          props = props
        )
      }
      .getOrElse(SchemaRegistryConfig())
}
