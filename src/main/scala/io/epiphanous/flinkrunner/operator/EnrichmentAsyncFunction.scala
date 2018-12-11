package io.epiphanous.flinkrunner.operator

import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.epiphanous.flinkrunner.model.FlinkConfig
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * An abstract asynchronous function to enrich a data stream with non-stream data. This
  * class relies on guava's [[CacheBuilder]] to load and cache the enrichment data. The
  * default cache loader assumes the cache key is a uri of a json api endpoint which it
  * loads asynchronously and converts to the cache value type (CV). You can provide your
  * own cache loader to load enrichment data in some other way. If you use the default
  * loader, note you must implicitly provide a circe [[EntityDecoder]] to convert the
  * json api result body to the cache value type.
  *
  * The behavior of the function can be parameterized with flink config values for
  *
  *   - $configPrefix.num.threads (size of thread pool, Int)
  *   - $configPrefix.cache.concurrency.level (Int)
  *   - $configPrefix.cache.max.size (max number of records in cache, Int)
  *   - $configPrefix.cache.expire.after (Duration)
  *
  * The cache always uses weak keys, allowing for aggressive garbage collection of
  * unused values.
  *
  * Subclasses must implement the [[getCacheKey()]] and [[enrichEvent()]] methods.
  *
  * @param configPrefix for extracting configuration information
  * @param cacheLoaderOpt an optional CacheLoader for loading the enrichment data
  * @param config implicit flink config
  * @param decoder implicit entity decoder for converting the body of the api call to the cache value type
  * @tparam IN the input stream element type
  * @tparam OUT the enriched stream output element type
  * @tparam CV the cache value type
  */
abstract class EnrichmentAsyncFunction[IN, OUT, CV <: AnyRef](
  configPrefix: String,
  cacheLoaderOpt: Option[CacheLoader[String, Option[CV]]] = None
)(implicit config: FlinkConfig,
  decoder: Decoder[CV])
    extends AsyncFunction[IN, OUT]
    with LazyLogging {

  @transient
  lazy implicit val entityDecoder: EntityDecoder[IO, CV] = jsonOf[IO, CV]

  /**
    * A thread pool execution context for making asynchronous api calls.
    */
  @transient
  lazy implicit val ec: ExecutionContext = new ExecutionContext {
    val threadPool =
      Executors.newFixedThreadPool(Math.max(2, config.getInt(s"$configPrefix.num.threads")))

    def execute(runnable: Runnable): Unit =
      threadPool.submit(runnable)

    def reportFailure(t: Throwable): Unit =
      logger.error(t.getLocalizedMessage)
  }

  @transient
  lazy implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  @transient
  lazy implicit val timer: Timer[IO] = IO.timer(ec)

  @transient
  lazy val api = BlazeClientBuilder[IO](ec).resource

  /**
    * The default cache loader implementation. This uses a blaze client to make an api call
    * and converts the result to the cache value type (CV). This loader will be used unless
    * one is passed into the class constructor (usually just done for testing).
    */
  @transient
  lazy val defaultCacheLoader = new CacheLoader[String, Option[CV]] {
    override def load(uri: String): Option[CV] = {
      api
        .use { client =>
          client.expect[CV](uri).attempt
        }
        .unsafeRunSync() match {
        case Left(failure) =>
          logger.error(s"Can't load key $uri: ${failure.getMessage}")
          None
        case Right(value) => Some(value)
      }
    }
  }

  @transient
  lazy val cache = {
    val builder = CacheBuilder
      .newBuilder()
      .concurrencyLevel(config.getInt(s"$configPrefix.cache.concurrency.level"))
      .maximumSize(config.getInt(s"$configPrefix.cache.max.size"))
      .expireAfterWrite(config.getDuration(s"$configPrefix.cache.expire.after"))
    if (!config.getBoolean(s"$configPrefix.cache.use.strong.keys"))
      builder.weakKeys()
    if (config.getBoolean(s"$configPrefix.cache.record.stats"))
      builder.recordStats()
    builder.build[String, Option[CV]](cacheLoaderOpt.getOrElse(defaultCacheLoader))
  }

  /**
    * Getter for $configPrefix value
    * @return
    */
  def getConfigPrefix = configPrefix

  override def asyncInvoke(in: IN, collector: ResultFuture[OUT]): Unit =
    asyncInvokeF(in) map {
      case Failure(throwable) => collector.completeExceptionally(throwable)
      case Success(results)   => collector.complete(results)
    }

  /**
    * A helper method to enable testing of [[asyncInvoke()]] without needing to construct a flink [[ResultFuture]] collector.
    * @param in the input event
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
    * Generate the cache key from the input event. For the default cache loader implementation,
    * this should be a json api endpoint uri. If you provide your own cache loader implementation,
    * this should be whatever is appropriate, however, it must be a [[String]].
    * @param in the input event
    * @return
    */
  def getCacheKey(in: IN): String

  /**
    * Construct a sequence of zero or more enriched output events using the input event and the api results.
    * @param in input event
    * @param data some api results (or none if api call failed)
    * @return
    */
  def enrichEvent(in: IN, data: Option[CV]): Seq[OUT]

}
