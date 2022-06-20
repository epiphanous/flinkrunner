package io.epiphanous.flinkrunner.operator

import io.circe.generic.auto._
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.FlinkConfig

class EnrichmentAsyncFunctionSpec extends PropSpec {

  case class MyOrigin(origin: String)

  property("defaultCacheLoader") {
    val config = new FlinkConfig(
      Array("testcache"),
      Some("""
        |prefix.cache {
        |}
        |""".stripMargin)
    )
    val eaf    = {
      new EnrichmentAsyncFunction[String, String, String, MyOrigin](
        "prefix",
        config
      ) {
        override def getCacheKey(in: String): String = in
        override def enrichEvent(
            in: String,
            data: Option[MyOrigin]): Seq[String] =
          Seq(in + ":" + data.map(o => o.origin).getOrElse("not-found"))
      }
    }
    val result = eaf.cache
      .get("http://httpbin.org/ip")
      .map { o =>
        logger.debug(s"cache call returned $o")
        o.origin should fullyMatch regex """\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"""
        o
      }
    eaf.cache.getIfPresent("http://httpbin.org/ip") shouldEqual result
  }
}
