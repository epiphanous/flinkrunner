package io.epiphanous.flinkrunner.util

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.generic.auto._
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.util.aws.AWSSigner
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.dsl.io._
import org.http4s.dsl.io.POST
import org.http4s.{EntityDecoder, EntityEncoder, Request, Uri}

class AWSSignerSpec extends PropSpec {

  case class Dingo(name: String, age: Int)
  object Dingo {
    implicit val dingoEnc: EntityEncoder[IO, Dingo] = jsonEncoderOf
    implicit val dingoDec: EntityDecoder[IO, Dingo] = jsonOf
  }

  ignore("sign property") {
    val request: Request[IO] = POST(
      Uri
        .unsafeFromString(
          "s3://msc-sandbox-test-bucket/rfl/system/Configuration_202205220900.json"
        )
    ).withEntity(new Dingo("jim", 17))
    val signer               = new AWSSigner(request)
    val signedRequest        = signer.sign
    signedRequest.headers.foreach(println)
    println(
      "Body:\n" +
        signedRequest.bodyText.bufferAll.compile.last
          .map(_.getOrElse(""))
          .unsafeRunSync()
    )
    println(signedRequest.toString())
  }

}
