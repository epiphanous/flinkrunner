package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

class SchemaRegistryConfigSpec extends PropSpec {

  property("use logical types") {
    val config = new FlinkConfig(
      Array("testJob"),
      Some("""
        |sources {
        |  my-kafka-source {
        |    schema.registry {
        |      url = "bogus"
        |      specific.avro.reader = false
        |      avro.use.logical.type.converters = false
        |    }
        |  }
        |}
        |""".stripMargin)
    )

    val schemaRegistryConfig = SchemaRegistryConfig.create(
      deserialize = true,
      config.getObjectOption("sources.my-kafka-source.schema.registry")
    )

    println(schemaRegistryConfig)
    schemaRegistryConfig.success.value.props
      .get("schema.registry.url") shouldEqual "bogus"
    schemaRegistryConfig.success.value.props
      .get("specific.avro.reader") shouldEqual "false"
    schemaRegistryConfig.success.value.props
      .get("avro.use.logical.type.converters") shouldEqual "false"
  }
}
