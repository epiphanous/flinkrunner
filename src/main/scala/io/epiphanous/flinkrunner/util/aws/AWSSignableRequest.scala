package io.epiphanous.flinkrunner.util.aws

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.http.HttpMethodName
import com.amazonaws.{ReadLimitInfo, SignableRequest}
import com.typesafe.scalalogging.LazyLogging
import org.http4s.{Header, Request}
import org.typelevel.ci.CIString

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** Simple wrapper around an Http4s request to enable us to sign it using
  * the AWS java sdk for AWS v4 signing.
  * @param request
  *   the unsigned Request[IO]
  */
class AWSSignableRequest(request: Request[IO])
    extends SignableRequest[Request[IO]]
    with LazyLogging {

  val headers = mutable.Map.empty[String, String]
  val params  = mutable.Map.empty[String, ArrayBuffer[String]]

  def getSignedRequest: Request[IO] = {
    request
      .withHeaders(headers.map { case (h, v) =>
        Header.Raw(CIString(h), v)
      }.toList)
      .withUri(params.foldLeft(request.uri) { case (u, (p, v)) =>
        u.withQueryParam(p, v)
      })
  }

  override def addHeader(name: String, value: String): Unit =
    headers += name -> value

  override def addParameter(name: String, value: String): Unit = {
    params.find { case (p, _) => p.equalsIgnoreCase(name) } match {
      case Some((_, v)) => v += value
      case None         => params += name -> ArrayBuffer(value)
    }
  }

  // not used
  override def setContent(content: InputStream): Unit = {}

  override def getHeaders: util.Map[String, String] =
    (request.headers.headers
      .map(raw => (raw.name.toString, raw.value))
      .toMap ++ headers).asJava

  override def getResourcePath: String = request.uri.path.toString()

  override def getParameters: util.Map[String, util.List[String]] =
    request.uri.multiParams.map { case (k, v) => (k, v.asJava) }.asJava

  override def getEndpoint: URI = new URI(
    Seq(
      request.uri.scheme.map(_.value),
      request.uri.host.map(_.value)
    ).flatten.mkString("://")
  )

  override def getHttpMethod: HttpMethodName =
    HttpMethodName.fromValue(request.method.name)

  override def getTimeOffset: Int = 0

  override def getContent: InputStream =
    request.body.bufferAll.compile.toVector
      .map(bytes => new ByteArrayInputStream(bytes.toArray))
      .unsafeRunSync()

  override def getContentUnwrapped: InputStream = getContent

  override def getReadLimitInfo: ReadLimitInfo = new ReadLimitInfo {
    override def getReadLimit: Int = (1 << 17) + 1
  }

  override def getOriginalRequestObject: AnyRef = request
}
