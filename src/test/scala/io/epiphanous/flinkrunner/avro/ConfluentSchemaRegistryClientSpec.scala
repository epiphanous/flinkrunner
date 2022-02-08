package io.epiphanous.flinkrunner.avro

import io.epiphanous.flinkrunner.BasePropSpec

class ConfluentSchemaRegistryClientSpec extends BasePropSpec {

  property("cacheLoader") {
    val client = new ConfluentSchemaRegistryClient()
    client.cacheLoader.load("https://httpbin.org/ip")
  }

}
