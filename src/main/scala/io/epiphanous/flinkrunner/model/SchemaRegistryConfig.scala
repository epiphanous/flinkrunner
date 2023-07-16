package io.epiphanous.flinkrunner.model

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.typesafe.config.{
  ConfigFactory,
  ConfigObject,
  ConfigValueFactory
}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.epiphanous.flinkrunner.model.SchemaRegistryType.{
  AwsGlue,
  Confluent
}
import io.epiphanous.flinkrunner.util.ConfigToProps.RichConfigObject
import io.epiphanous.flinkrunner.util.StreamUtils.RichProps

import java.util
import scala.util.{Failure, Success, Try}

case class SchemaRegistryConfig(
    schemaRegistryType: SchemaRegistryType,
    isDeserializing: Boolean = false,
    cacheCapacity: Int = 1000,
    props: util.Map[String, String] = new util.HashMap(),
    headers: util.Map[String, String] = new util.HashMap()) {
  val url: String =
    props.getOrDefault("schema.registry.url", "na")
}

object SchemaRegistryConfig {

  val DEFAULT_SCHEMA_REG_URL = "http://localhost:8082"
  val DEFAULT_CACHE_CAPACITY = 1000

  def create(
      deserialize: Boolean,
      configOpt: Option[ConfigObject]): Try[SchemaRegistryConfig] = {
    val config                = configOpt
      .map(_.toConfig)
      .getOrElse(
        ConfigFactory
          .parseString(
            s"""schema.registry.url = "$DEFAULT_SCHEMA_REG_URL""""
          )
          .getConfig("schema.registry")
      )
    val headers               = Try(
      config.getObject("headers")
    ).toOption.asProperties.asJavaMap
    val explicitType          = Try(config.getString("type")).toOption
      .flatMap(SchemaRegistryType.withNameInsensitiveOption)
    val secondaryDeserializer = Try(
      config.getString(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)
    ).toOption
    if (
      explicitType.contains(Confluent) || (config.hasPath(
        "url"
      ) && secondaryDeserializer.isEmpty)
    ) {
      val url                = Try(config.getString("url"))
        .getOrElse(DEFAULT_SCHEMA_REG_URL)
      val useLogicalTypes    = Try(
        config.getBoolean(
          KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG
        )
      ).getOrElse(true)
      val cacheCapacity: Int = Try(config.getInt("cache.capacity"))
        .getOrElse(DEFAULT_CACHE_CAPACITY)
      val props              = config
        .withoutPath("url")
        .withoutPath("cache.capacity")
        .withoutPath("headers")
        .withValue(
          "schema.registry.url",
          ConfigValueFactory.fromAnyRef(url)
        )
        .withValue(
          KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG,
          ConfigValueFactory.fromAnyRef(useLogicalTypes)
        )
        .root()
      Success(
        SchemaRegistryConfig(
          Confluent,
          isDeserializing = deserialize,
          cacheCapacity = cacheCapacity,
          headers = headers,
          props = Some(props).asProperties.asJavaMap
        )
      )
    } else if (
      explicitType.contains(AwsGlue) ||
      config.hasPath(AWSSchemaRegistryConstants.AWS_REGION) || config
        .hasPath(AWSSchemaRegistryConstants.AWS_ENDPOINT)
    ) {
      Success(
        SchemaRegistryConfig(
          AwsGlue,
          isDeserializing = deserialize,
          headers = headers,
          props = Some(config.root()).asProperties.asJavaMap
        )
      )
    } else {
      Failure(new RuntimeException("invalid schema registry config"))
    }
  }
}
