package io.epiphanous.flinkrunner.operator

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.streaming.api.scala.async.{
  AsyncFunction,
  ResultFuture
}
import org.apache.flink.util.concurrent.Executors
import org.http4s.EntityDecoder
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.jsonOf
import org.http4s.client.Client

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
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
    CV <: AnyRef,
    ADT <: FlinkEvent](
    configPrefix: String,
    cacheLoaderOpt: Option[CacheLoader[String, Option[CV]]] = None,
    preloaded: Map[String, CV] = Map.empty[String, CV],
    config: FlinkConfig
)(implicit decoder: Decoder[CV])
    extends AsyncFunction[IN, OUT]
    with LazyLogging {

  @transient
  lazy implicit val entityDecoder: EntityDecoder[IO, CV] = jsonOf[IO, CV]

  @transient
  lazy implicit val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.directExecutor())

//  @transient
//  lazy implicit val cs: ContextShift[IO] = IO.contextShift(ec)

//  @transient
//  lazy implicit val timer: Timer[IO] = IO.timer(ec)

  @transient
  lazy val api: Resource[IO, Client[IO]] = BlazeClientBuilder[IO].resource

  /**
   * The default cache loader implementation. This uses a blaze client to
   * make an api call and converts the result to the cache value type (CV).
   * This loader will be used unless one is passed into the class
   * constructor (usually just done for testing).
   */
  @transient
  lazy val defaultCacheLoader: CacheLoader[String, Option[CV]] =
    new CacheLoader[String, Option[CV]] {
      override def load(uri: String): Option[CV] = {
        logger.debug(s"=== cache load $uri")
        preloaded.get(uri) match {
          case Some(cv) => Some(cv)
          case None     =>
            api
              .use { client =>
                client.expect[CV](uri).attempt
              }
              .unsafeRunSync() match {
              case Left(failure) =>
                logger.error(s"Can't load key $uri: ${failure.getMessage}")
                None
              case Right(value)  => Some(value)
            }
        }
      }
    }

  @transient
  lazy val cache: LoadingCache[String, Option[CV]] = {
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
    builder.build[String, Option[CV]](
      cacheLoaderOpt.getOrElse(defaultCacheLoader)
    )
  }

  /**
   * Getter for configPrefix value
   *
   * @return
   */
  def getConfigPrefix: String = configPrefix

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
        enrichEvent(in, data)
      }
    }

  /**
   * Generate the cache key from the input event. For the default cache
   * loader implementation, this should be a json api endpoint uri. If you
   * provide your own cache loader implementation, this should be whatever
   * is appropriate, however, it must be a String.
   *
   * @param in
   *   the input event
   * @return
   */
  def getCacheKey(in: IN): String

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
