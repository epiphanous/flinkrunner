package io.epiphanous.flinkrunner.util.aws

import cats.effect.IO
import org.http4s.client.dsl.io._
import org.http4s.dsl.io.{GET, POST}
import org.http4s.{EntityEncoder, Headers, Request, Uri}

object Requests {

  /** Creates an http4s GET Request[IO] that has been signed with an AWS v4
    * signature.
    * @param uri
    *   the full uri of the request as a string (recognizes
    *   <code>s3://bucket/object</code> urls)
    * @param headers
    *   an optional map of headers
    * @return
    *   Request[IO] signed for the requested AWS service
    */
  def get(
           awsEndpoint:Option[String] = None,
      uri: String,
      headers: Map[String, String] = Map.empty): Request[IO] =
    new AWSSigner(
      GET(Uri.unsafeFromString(uri), Headers(headers.toSeq)),awsEndpoint

    ).sign

  /** Creates an http4s POST Request[IO] that has been signed with an AWS
    * v4 signature. The caller must provide an entity decoder for the body
    * type A. Http4s has many built in implicit entity decoders which you
    * can bring into scope for bodies of different content types.
    * @param body
    *   the body of the request
    * @param uri
    *   the full uri of the request as a string (recognizes
    *   <code>s3://bucket/object</code> urls)
    * @param headers
    *   an optional map of headers
    * @param w
    *   an implicit entity encoder
    * @tparam A
    *   the type of the body object
    * @return
    *   Request[IO] signed for the requested AWS service
    */
  def post[A](
      body: A,
      uri: String,
      headers: Map[String, String] = Map.empty)(implicit
      w: EntityEncoder[IO, A]): Request[IO] =
    new AWSSigner(
      POST(body, Uri.unsafeFromString(uri), Headers(headers.toSeq))
    ).sign
}
