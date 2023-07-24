package io.epiphanous.flinkrunner.util

import cats.effect.IO
import com.amazonaws.auth.BasicAWSCredentials
import com.google.common.hash.Hashing
import io.circe.generic.auto._
import io.circe.syntax._
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.util.aws.AWSSigner
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.dsl.io._
import org.http4s.dsl.io.POST
import org.http4s.{EntityDecoder, EntityEncoder, Request, Uri}
import org.typelevel.ci.CIString

import java.nio.charset.StandardCharsets

class AWSSignerSpec extends PropSpec {

  case class TestBody(name: String, age: Int)
  object TestBody {
    implicit val testBodyEnc: EntityEncoder[IO, TestBody] = jsonEncoderOf
    implicit val testBodyDec: EntityDecoder[IO, TestBody] = jsonOf
  }

  def getHeader(req: Request[IO], name: String): Option[String] =
    req.headers
      .get(CIString(name))
      .map(h => h.last.value)


  property("sign property") {
    val testBody             = TestBody("horatio", 32)
    val testBodyContent      = testBody.asJson.noSpaces
    val request: Request[IO] = POST(
      Uri
        .unsafeFromString(
          "s3://my-bucket/some/path/to_a_file.json"
        )
    ).withEntity(testBody)
    val signer               = new AWSSigner(
      request = request,
      providedCredentials =
        Some(new BasicAWSCredentials("foobar", "foobaz"))
    )
    val signedRequest        = signer.sign
    val expectedDigest       = Hashing
      .sha256()
      .hashString(testBodyContent, StandardCharsets.UTF_8)
      .toString

    getHeader(
      signedRequest,
      "x-amz-content-sha256"
    ).value shouldEqual expectedDigest

    getHeader(
      signedRequest,
      "Host"
    ).value shouldEqual "my-bucket.s3.amazonaws.com"

    val signedRequestLocal = new AWSSigner(
      request = request,
      providedCredentials =
        Some(new BasicAWSCredentials("foobar", "foobaz")),serviceAndEndpoint=Some(("s3",Some(Uri.unsafeFromString(s"http://localstack:4566/msgbus-green/sandbox/schema/default/") )))
    ).sign
    getHeader(
      signedRequestLocal,
      "Host"
    ).value shouldEqual "localstack"
    signedRequestLocal.uri.toString() shouldEqual "http://localstack:4566/msgbus-green/sandbox/schema/default/"

//    signedRequest.headers.foreach(println)
//    println(
//      "Body:\n" +
//        signedRequest.bodyText.bufferAll.compile.last
//          .map(_.getOrElse(""))
//          .unsafeRunSync()
//    )
//    println(signedRequest.toString())
  }

}
