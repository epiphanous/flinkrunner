package io.epiphanous.flinkrunner.operator

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.epiphanous.flinkrunner.model.FlinkConfig
import org.apache.flink.streaming.api.scala.async.{
  AsyncFunction,
  ResultFuture
}
import org.apache.flink.util.concurrent.Executors
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Request, Uri}

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.TimeUnit
import scala.concurrent.{
  ExecutionContext,
  ExecutionContextExecutor,
  Future
}
import scala.util.{Failure, Success, Try}

/**
 * An abstract asynchronous function to enrich a data stream with
 * non-stream data. This class relies on guava's CacheBuilder to load and
 * cache the enrichment data. The default cache loader assumes the cache
 * key is a uri of a json api endpoint which it loads asynchronously and
 * converts to the cache value type (CV). You can provide your own cache
 * loader to load enrichment data in some other way. If you use the default
 * loader, note you must implicitly provide a circe EntityDecoder to
 * convert the json api result body to the cache value type.
 *
 * The behavior of the function can be parameterized with flink config
 * values for the following variables (relative to the configPrefix):
 *
 *   - num.threads (size of thread pool, Int)
 *   - cache.concurrency.level (Int)
 *   - cache.max.size (max number of records in cache, Int)
 *   - cache.expire.after (Duration)
 *
 * The cache always uses weak keys, allowing for aggressive garbage
 * collection of unused values.
 *
 * Subclasses must implement the getCacheKey() and enrichEvent() methods.
 *
 * @param configPrefix
 *   for extracting configuration information
 * @param cacheLoaderOpt
 *   an optional CacheLoader for loading the enrichment data
 * @param config
 *   implicit flink config
 * @param decoder
 *   implicit entity decoder for converting the body of the api call to the
 *   cache value type
 * @tparam IN
 *   the input stream element type
 * @tparam OUT
 *   the enriched stream output element type
 * @tparam CV
 *   the cache value type
 */
abstract class EnrichmentAsyncFunction[
    IN,
    OUT,
    CK <: AnyRef,
    CV >: Null <: AnyRef](
    configPrefix: String,
    config: FlinkConfig,
    cacheLoaderOpt: Option[CacheLoader[CK, CV]] = None,
    preloaded: Map[CK, CV] = Map.empty[CK, CV]
)(implicit decoder: Decoder[CV])
    extends AsyncFunction[IN, OUT]
    with LazyLogging {

  @transient
  lazy implicit val entityDecoder: EntityDecoder[IO, CV] = jsonOf[IO, CV]

  @transient
  lazy implicit val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  @transient
  lazy val api: Resource[IO, Client[IO]] = BlazeClientBuilder[IO].resource

  /**
   * The default cache loader implementation. This uses a blaze client to
   * make an api call and converts the result to the cache value type (CV).
   * This loader will be used unless one is passed into the class
   * constructor (usually just done for testing).
   */
  @transient
  lazy val defaultCacheLoader =
    new CacheLoader[CK, CV] {
      override def load(cacheKey: CK): CV = {
        logger.debug(s"=== cache load $cacheKey")
        preloaded.get(cacheKey) match {
          case Some(cv) => cv
          case None     =>
            api
              .use { client =>
                client.expect[CV](requestFor(cacheKey)).attempt
              }
              .unsafeRunSync() match {
              case Left(failure) =>
                logger.error(
                  s"Can't load key [$cacheKey]: ${failure.getMessage}"
                )
                null
              case Right(value)  => value
            }
        }
      }
    }

  @transient
  lazy val cache: LoadingCache[CK, CV] = {
    logger.debug("=== initializing new cache")
    val expireAfter =
      config.getDuration(s"$configPrefix.cache.expire.after")
    val builder     = CacheBuilder
      .newBuilder()
      .concurrencyLevel(
        config.getInt(s"$configPrefix.cache.concurrency.level")
      )
      .maximumSize(config.getLong(s"$configPrefix.cache.max.size"))
      .expireAfterWrite(expireAfter.toMillis, TimeUnit.MILLISECONDS)
    //      .expireAfterWrite(expireAfter) // for guava 27
    if (!config.getBoolean(s"$configPrefix.cache.use.strong.keys"))
      builder.weakKeys()
    if (config.getBoolean(s"$configPrefix.cache.record.stats"))
      builder.recordStats()
    builder.build[CK, CV](
      cacheLoaderOpt.getOrElse(defaultCacheLoader)
    )
  }

  /**
   * Getter for configPrefix value
   *
   * @return
   */
  def getConfigPrefix: String = configPrefix

  /**
   * Return an http4s [[Request]] [IO] for the requested cache key.
   * Implementers can override this to handle setting the headers or body
   * of the request based on the requested key. The default method here
   * simply makes a simple get request to the cacheKey as if it were a full
   * HTTP url.
   * @param cacheKey
   *   the cache key we're constructing a request to load (treated here as
   *   a json endpoint url)
   * @return
   *   an http4s [[Request]] [IO]
   */
  def requestFor(cacheKey: CK): Request[IO] =
    Request[IO](uri = Uri.unsafeFromString(cacheKey.toString))

  /**
   * Flink entry for invoking the async enrichment function
   *
   * @param in
   *   the input event
   * @param collector
   *   a result future for the enriched output
   */
  override def asyncInvoke(in: IN, collector: ResultFuture[OUT]): Unit =
    asyncInvokeF(in) foreach {
      case Failure(throwable) =>
        val sw = new StringWriter()
        throwable.printStackTrace(new PrintWriter(sw))
        logger.error(
          s"asyncInvoke[$in] failed: ${throwable.getMessage}\n$sw"
        )
        collector.completeExceptionally(throwable)
      case Success(results)   => collector.complete(results)
    }

  /**
   * A helper method to enable testing of asyncInvoke() without needing to
   * construct a flink ResultFuture collector.
   *
   * @param in
   *   the input event
   * @return
   */
  def asyncInvokeF(in: IN): Future[Try[Seq[OUT]]] =
    Future {
      Try {
        val data = cache.get(getCacheKey(in))
        enrichEvent(in, Option(data))
      }
    }

  /**
   * Generate the cache key from the input event. This must be provided by
   * the implementor.
   *
   * This key works together with the [[requestFor]] method to construct an
   * appropriate HTTP request to load the cache value appropriate for the
   * cacheKey. The default [[requestFor]] method simply uses the cache key
   * as a url and makes a simple GET request assuming that URL is a json
   * endpoint. However, implementors can override the [[requestFor]] method
   * to construct whatever kind of HTTP request is need to load the cache
   * value.
   *
   * @param in
   *   the input event
   * @return
   *   the cache key
   */
  def getCacheKey(in: IN): CK

  /**
   * Construct a sequence of zero or more enriched output events using the
   * input event and the api results.
   *
   * @param in
   *   input event
   * @param data
   *   some api results (or none if api call failed)
   * @return
   */
  def enrichEvent(in: IN, data: Option[CV]): Seq[OUT]

}
