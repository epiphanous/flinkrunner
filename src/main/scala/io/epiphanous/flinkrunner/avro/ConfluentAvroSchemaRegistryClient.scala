package io.epiphanous.flinkrunner.avro

import cats.effect.{ContextShift, IO, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.epiphanous.flinkrunner.model.FlinkConfig
import io.epiphanous.flinkrunner.util.StringUtils
import org.apache.avro.Schema.Parser
import org.apache.flink.runtime.concurrent.Executors.directExecutionContext
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf
import org.http4s.client.blaze.BlazeClientBuilder

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class ConfluentAvroSchemaRegistryClient(
    preloaded: Map[String, RegisteredAvroSchema] = Map.empty
)(implicit
    config: FlinkConfig,
    decoder: Decoder[ConfluentAvroSchemaRegistryResponse])
    extends AvroSchemaRegistryClient
    with StringUtils
    with LazyLogging {

  import ConfluentAvroSchemaRegistryClient.configPrefix

  @transient
  lazy implicit val entityDecoder
      : EntityDecoder[IO, ConfluentAvroSchemaRegistryResponse] =
    jsonOf[IO, ConfluentAvroSchemaRegistryResponse]

  @transient
  lazy implicit val ec: ExecutionContext = directExecutionContext()

  @transient
  lazy implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  @transient
  lazy implicit val timer: Timer[IO] = IO.timer(ec)

  @transient
  lazy val api = BlazeClientBuilder[IO](ec).resource

  @transient
  lazy val parser = new Parser()

  @transient
  lazy val urlBase     = config.getString(s"$configPrefix.url.base")
  @transient
  lazy val cacheLoader =
    new CacheLoader[String, Try[RegisteredAvroSchema]] {
      override def load(uri: String): Try[RegisteredAvroSchema] = {
        logger.debug(s"=== confluent schema registry cache load $uri")
        preloaded.get(uri) match {
          case Some(pre) => Success(pre)
          case None      =>
            api
              .use { client =>
                client
                  .expect[ConfluentAvroSchemaRegistryResponse](uri)
                  .attempt
              }
              .unsafeRunSync() match {
              case Left(failure)   =>
                logger.error(s"Can't load key $uri: ${failure.getMessage}")
                Failure(failure)
              case Right(response) =>
                response match {
                  case byId: ConfluentAvroSchemaRegistryResponseById                         =>
                    val id     = uri.split("/").last.toInt
                    val schema = parser.parse(byId.schema)
                    Success(
                      RegisteredAvroSchema(id, schema, schema.getFullName)
                    )
                  case bySubjectVersion: ConfluentAvroSchemaRegistryResponseBySubjectVersion =>
                    val schema = parser.parse(bySubjectVersion.schema)
                    Success(
                      RegisteredAvroSchema(
                        bySubjectVersion.id,
                        schema,
                        bySubjectVersion.subject,
                        bySubjectVersion.version
                      )
                    )
                }
            }
        }
      }
    }

  @transient
  lazy val cache = {
    logger.debug("=== initializing new confluent schema registry cache")
    val expireAfter =
      config.getDuration(s"$configPrefix.cache.expire.after")
    val builder     = CacheBuilder
      .newBuilder()
      .concurrencyLevel(
        config.getInt(s"$configPrefix.cache.concurrency.level")
      )
      .maximumSize(config.getInt(s"$configPrefix.cache.max.size"))
      .expireAfterWrite(expireAfter.toMillis, TimeUnit.MILLISECONDS)
    //      .expireAfterWrite(expireAfter) // for guava 27
    if (!config.getBoolean(s"$configPrefix.cache.use.strong.keys"))
      builder.weakKeys()
    if (config.getBoolean(s"$configPrefix.cache.record.stats"))
      builder.recordStats()
    builder.build[String, Try[RegisteredAvroSchema]](cacheLoader)
  }

  /**
   * Gets a schema from the registry by id.
   *
   * @param id
   *   the id of the scheme to fetch
   * @return
   *   the registered schema wrapped in a Try
   */
  override def get(id: Int) =
    cache.get(url(id))

  /**
   * Gets the latest schema from the registry based on its subject. If
   * retrieved successfully, also ensures the schema is installed in the
   * cache by its unique id.
   *
   * @param subject
   *   the name of the schema
   * @return
   *   the registered schema wrapped in a Try
   */
  override def get(subject: String) =
    cache.get(url(subject)).map(putCache)

  /**
   * Get a the most recent schema associated with the specified event. If
   * retrieved successfully, also ensures the schema is installed in the
   * cache by its unique id.
   *
   * @param event
   *   the event
   * @param isKey
   *   indicates whether you want the key or value schema
   * @tparam E
   *   the event class
   * @return
   *   the registered schema wrapped in a Try
   */
  override def get[E](event: E, isKey: Boolean = false) =
    cache.get(url(subject(event, isKey))).map(putCache)

  /**
   * Return the registry subject name of the schema associated with the
   * provided event instance.
   *
   * @param event
   *   an instance of E
   * @param isKey
   *   if true add '_key' else '_value' suffix
   * @tparam E
   *   event class
   * @return
   *   schema subject name
   */
  override def subject[E](event: E, isKey: Boolean = false): String =
    (event.getClass.getCanonicalName.split("\\.")
      :+ (if (isKey) "key" else "value"))
      .map(snakify)
      .map(name => clean(name, replacement = "_"))
      .mkString("_")

  /**
   * Ensure the schema is installed in the cache under its id. Used by some
   * get() methods which are name based.
   *
   * @param schema
   *   a registered avro schema instance
   * @return
   *   the schema sent in
   */
  protected def putCache(
      schema: RegisteredAvroSchema): RegisteredAvroSchema = {
    cache.put(url(schema.id), Success(schema))
    schema
  }

  /**
   * Return the api endpoint used to retrieve a schema by id
   *
   * @param id
   *   the id of the schema to retrieve
   * @return
   *   the endpoint url
   */
  protected def url(id: Int): String = s"$urlBase/schemas/ids/$id"

  /**
   * Return the api endpoint used to retrieve a schema by its subject name
   * and version string.
   *
   * @param subject
   *   the full name of the schema
   * @param version
   *   the version to retrieve (defaults to "latest")
   * @return
   */
  protected def url(subject: String, version: String = "latest"): String =
    s"$urlBase/subjects/$subject/versions/$version"

}

object ConfluentAvroSchemaRegistryClient {
  final val configPrefix = "confluent.avro.schema.registry"
}
