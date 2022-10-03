package io.epiphanous.flinkrunner.util.aws

import cats.effect.IO
import com.amazonaws.auth.internal.SignerConstants
import com.amazonaws.auth.{
  AWS4Signer,
  AWSCredentials,
  DefaultAWSCredentialsProviderChain
}
import com.typesafe.scalalogging.LazyLogging
import org.http4s.{Header, Request, Uri}
import org.typelevel.ci.CIString

import scala.util.matching.Regex

class AWSSigner(
    request: Request[IO],
    providedCredentials: Option[AWSCredentials] = None)
    extends LazyLogging {

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

  val credentials: AWSCredentials = providedCredentials.getOrElse(
    DefaultAWSCredentialsProviderChain.getInstance().getCredentials
  )

  def sign: Request[IO] = {
    signer.sign(signable, credentials)
    val req = signable.getSignedRequest
    logger.debug(s"AWS Signer: $req")
    logger.debug(
      s"AWS Signature: ${req.headers.get(CIString("Authorization"))}"
    )
    req
  }

  private val serviceEndpointPattern: Regex =
    raw"([^.]+)(\.[^.]+)?\.amazonaws\.com".r

  protected def resolveAWSService: Option[(String, Option[Uri])] = for {
    schema <- request.uri.scheme.map(_.value)
    host <- request.uri.host.map(_.value)
    (service, uri) <-
      if (schema.startsWith("s3")) {
        Some(
          "s3",
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
