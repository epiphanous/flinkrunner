package io.epiphanous.flinkrunner.util.aws

import cats.effect.IO
import com.amazonaws.auth.internal.SignerConstants
import com.amazonaws.auth.{
  AWS4Signer,
  AWSCredentials,
  DefaultAWSCredentialsProviderChain
}
import org.http4s.{Header, Request, Uri}
import org.typelevel.ci.CIString

import scala.util.matching.Regex

class AWSSigner(request: Request[IO]) {

  val Some(Tuple2(service, maybeUri)) = resolveAWSService

  val doubleUrlEncoding: Boolean = !service.equals("s3")

  val signer: AWS4Signer = new AWS4Signer(doubleUrlEncoding)
  signer.setServiceName(service)

  val signable: AWSSignableRequest = new AWSSignableRequest(
    maybeUri
      .map(uri =>
        request
          .withUri(uri)
          // our s3 signer needs this to add this required header during signing
          .putHeaders(
            Header.Raw(
              CIString(SignerConstants.X_AMZ_CONTENT_SHA256),
              "required"
            )
          )
      )
      .getOrElse(request)
  )

  val credentials: AWSCredentials =
    DefaultAWSCredentialsProviderChain.getInstance().getCredentials

  def sign: Request[IO] = {
    signer.sign(signable, credentials)
    signable.getSignedRequest
  }

  private val serviceEndpointPattern: Regex =
    raw"([^.]+)(\.[^.]+)?\.amazonaws\.com".r

  protected def resolveAWSService: Option[(String, Option[Uri])] = for {
    schema <- request.uri.scheme.map(_.value)
    host <- request.uri.host.map(_.value)
    (service, uri) <-
      if (schema.equals("s3")) {
        Some(
          schema,
          Some(
            Uri
              .unsafeFromString(s"https://$host.s3.amazonaws.com")
              .withPath(request.uri.path)
              .withMultiValueQueryParams(request.multiParams)
          )
        )
      } else {
        host match {
          case serviceEndpointPattern(servicePrefix, _) =>
            Some(servicePrefix, None)
          case _                                        =>
            Some("unknown", None)
        }
      }
  } yield (service, uri)

}
