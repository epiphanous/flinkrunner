package io.epiphanous.flinkrunner.avro

import cats.effect.{ContextShift, IO, Resource, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StringUtils
import org.apache.avro.Schema.Parser
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.concurrent.Executors
import org.http4s.EntityDecoder
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
class ConfluentSchemaRegistryClient[ADT <: FlinkEvent: TypeInformation](
    config: FlinkConfig[ADT])(implicit
    decoder: Decoder[ConfluentSchemaRegistryResponse])
    extends AvroSchemaRegistryClient[ConfluentSchemaRegistryContext]
    with StringUtils
    with LazyLogging {

  import ConfluentSchemaRegistryClient.configPrefix

  @transient
  lazy val preloaded: Map[String, RegisteredAvroSchema] = Map.empty

  @transient
  lazy implicit val entityDecoder
      : EntityDecoder[IO, ConfluentSchemaRegistryResponse] =
    jsonOf[IO, ConfluentSchemaRegistryResponse]

  @transient
  lazy implicit val ec: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.directExecutor())

  @transient
  lazy implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  @transient
  lazy implicit val timer: Timer[IO] = IO.timer(ec)

  @transient
  lazy val api: Resource[IO, Client[IO]] =
    BlazeClientBuilder[IO](ec).resource

  @transient
  lazy val parser = new Parser()

  @transient
  lazy val urlBase: String = config.getString(s"$configPrefix.url.base")

  @transient
  lazy val cacheLoader: CacheLoader[String, Try[RegisteredAvroSchema]] =
    new CacheLoader[String, Try[RegisteredAvroSchema]] {
      override def load(uri: String): Try[RegisteredAvroSchema] = {
        logger.debug(s"=== confluent schema registry cache load $uri")
        preloaded.get(uri) match {
          case Some(pre) => Success(pre)
          case None      =>
            api
              .use { client =>
                client
                  .expect[ConfluentSchemaRegistryResponse](uri)
                  .attempt
              }
              .unsafeRunSync() match {
              case Left(failure)   =>
                logger.error(s"Can't load key $uri: ${failure.getMessage}")
                Failure(failure)
              case Right(response) =>
                response match {
                  case byId: ConfluentSchemaRegistryResponseById                         =>
                    val id     = uri.split("/").last
                    val schema = parser.parse(byId.schema)
                    Success(RegisteredAvroSchema(schema, id))
                  case bySubjectVersion: ConfluentSchemaRegistryResponseBySubjectVersion =>
                    val schema = parser.parse(bySubjectVersion.schema)
                    Success(
                      RegisteredAvroSchema(
                        schema,
                        bySubjectVersion.id.toString,
                        Some(bySubjectVersion.subject),
                        Some(bySubjectVersion.version.toString)
                      )
                    )
                }
            }
        }
      }
    }

  @transient
  lazy val cache: LoadingCache[String, Try[RegisteredAvroSchema]] = {
    logger.debug("=== initializing new confluent schema registry cache")
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
    builder.build[String, Try[RegisteredAvroSchema]](cacheLoader)
  }

  /**
   * Extract the schema id from the first 5 bytes (first byte is magic 0x0,
   * next 4 is integer id), load the schema from the confluent registry and
   * return the schema and remaining encoded bytes.
   * @param message
   *   a byte array containing any schema identification as well as a
   *   binary avro encoded message
   * @param optContext
   *   the optional context (not used here)
   * @return
   *   a (schema, bytes) tuple, wrapped in a [[Try]]
   */
  override def getFromMessage(
      message: Array[Byte],
      optContext: Option[ConfluentSchemaRegistryContext] = None)
      : Try[(RegisteredAvroSchema, Array[Byte])] = for {
    buffer <- Try(ByteBuffer.wrap(message))
    _ <- Try {
           if (buffer.get == 0x0) Success(true)
           else
             Failure(
               new Throwable(
                 s"message bytes missing initial magic 0x0 byte"
               )
             )
         }
    id <- Try(buffer.getInt())
    schema <- cache
                .get(url(id))
                .map(schema =>
                  putCache(
                    schema,
                    url(
                      schema.subject,
                      optContext
                        .getOrElse(ConfluentSchemaRegistryContext())
                        .version
                    )
                  )
                )
    bytes <- Try {
               val b = new Array[Byte](buffer.remaining())
               buffer.get(b)
               b
             }
  } yield (schema, bytes)

  /**
   * @param event
   *   the event
   * @param optContext
   *   the optional context
   * @tparam E
   *   the type of event
   * @return
   *   a (schema, magic) tuple, wrapped in a [[Try]]
   */
  override def getFromEvent[E](
      event: E,
      optContext: Option[ConfluentSchemaRegistryContext] = None)
      : Try[(RegisteredAvroSchema, Array[Byte])] = for {
    schema <-
      cache
        .get(
          url(
            getSubjectName(event, optContext),
            optContext.getOrElse(ConfluentSchemaRegistryContext()).version
          )
        )
        .map(schema => putCache(schema, url(schema.id.toInt)))
    magic <- Try {
               ByteBuffer
                 .wrap(new Array[Byte](5))
                 .put(0x0.toByte)
                 .putInt(schema.id.toInt)
                 .array()
             }
  } yield (schema, magic)

  /**
   * Return the registry subject name of the schema associated with the
   * provided event instance.
   *
   * @param event
   *   an instance of E
   * @param optContext
   *   optional [[ConfluentSchemaRegistryContext]]
   * @tparam E
   *   event class
   * @return
   *   schema subject name
   */
  protected def getSubjectName[E](
      event: E,
      optContext: Option[ConfluentSchemaRegistryContext] = None)
      : String = {
    val keyOrValue  =
      if (optContext.getOrElse(ConfluentSchemaRegistryContext()).isKey)
        "key"
      else "value"
    val subjectName = config.getString(
      s"schema.registry.${event.getClass.getCanonicalName}"
    )
    s"$subjectName-$keyOrValue"
  }

  /**
   * Retrieve a schema based on its id or subject, and optionally, some
   * additional context information.
   *
   * @param idOrSubject
   *   the id or subject key
   * @param optContext
   *   the optional context
   * @return
   */
  override def get(
      idOrSubject: String,
      optContext: Option[ConfluentSchemaRegistryContext])
      : Try[RegisteredAvroSchema] = ???

  /**
   * Put the schema into the registry.
   *
   * @param schema
   *   a registered avro schema instance
   * @return
   *   the schema sent in
   */
  override def put(
      schema: RegisteredAvroSchema,
      optContext: Option[ConfluentSchemaRegistryContext] = None)
      : Try[RegisteredAvroSchema] = ???

  /**
   * Ensure a schema is in the cache at a provided key.
   * @param schema
   *   the schema to install
   * @param key
   *   the key to find the schema by
   * @return
   *   the schema
   */
  def putCache(
      schema: RegisteredAvroSchema,
      key: String): RegisteredAvroSchema = {
    cache.put(key, Success(schema))
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

object ConfluentSchemaRegistryClient {
  final val configPrefix = "confluent.avro.schema.registry"
}
