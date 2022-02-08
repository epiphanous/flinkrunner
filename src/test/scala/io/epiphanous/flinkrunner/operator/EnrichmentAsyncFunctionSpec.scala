package io.epiphanous.flinkrunner.operator

import io.circe.generic.auto._
import io.epiphanous.flinkrunner.PropSpec

class EnrichmentAsyncFunctionSpec extends PropSpec {

  case class MyOrigin(origin: String)

  property("defaultCacheLoader") {
    val eaf =
      new EnrichmentAsyncFunction[String, String, MyOrigin](
        "prefix",
        nothingConfig
      ) {
        override def getCacheKey(in: String): String = in
        override def enrichEvent(
            in: String,
            data: Option[MyOrigin]): Seq[String]     =
          Seq(in + ":" + data.map(o => o.origin).getOrElse("not-found"))
      }
    eaf.defaultCacheLoader
      .load("https://httpbin.org/ip")
      .value
      .origin should fullyMatch regex """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"""
  }
}
