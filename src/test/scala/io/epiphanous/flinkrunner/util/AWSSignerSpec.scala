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

  val testBody: TestBody      = TestBody("horatio", 32)
  val testBodyContent: String = testBody.asJson.noSpaces
  val creds                   = new BasicAWSCredentials("foobar", "foobaz")

  def getHeader(req: Request[IO], name: String): Option[String] =
    req.headers
      .get(CIString(name))
      .map(h => h.last.value)

  def getRequest(url: String): Request[IO] =
    POST(Uri.unsafeFromString(url)).withEntity(testBody)

  property("sign works") {
    val request: Request[IO] = getRequest(
      "s3://my-bucket/some/path/to_a_file.json"
    )
    val signer               =
      new AWSSigner(request = request, providedCredentials = Some(creds))
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
    signer.service shouldEqual "s3"
    signer.maybeUri
      .map(
        _.toString()
      )
      .value shouldEqual "https://my-bucket.s3.amazonaws.com/some/path/to_a_file.json"
  }

  property("local signer works") {
    val request            =
      getRequest("http://localstack:4566/some/path/to_a_file.json")
    val localSigner        = new AWSSigner(
      request = request,
      providedCredentials = Some(creds),
      serviceOpt = Some("s3")
    )
    val signedRequestLocal = localSigner.sign
    getHeader(
      signedRequestLocal,
      "Host"
    ).value shouldEqual "localstack"
    localSigner.maybeUri.value shouldEqual request.uri
    localSigner.service shouldEqual "s3"
    localSigner.credentials.getAWSAccessKeyId shouldEqual "foobar"
    localSigner.credentials.getAWSSecretKey shouldEqual "foobaz"

//    signedRequestLocal.headers.foreach(println)
//    println(
//      "Body:\n" +
//        signedRequestLocal.bodyText.bufferAll.compile.last
//          .map(_.getOrElse(""))
//          .unsafeRunSync()
//    )
//    println(signedRequestLocal.toString())
  }

}
