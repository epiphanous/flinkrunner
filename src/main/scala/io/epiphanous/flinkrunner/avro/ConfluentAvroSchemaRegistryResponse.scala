package io.epiphanous.flinkrunner.avro

import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

sealed trait ConfluentAvroSchemaRegistryResponse {
  def schema: String
}

case class ConfluentAvroSchemaRegistryResponseById(schema: String) extends ConfluentAvroSchemaRegistryResponse
object ConfluentAvroSchemaRegistryResponseById {
  implicit val encoder: Encoder[ConfluentAvroSchemaRegistryResponseById] =
    Encoder.forProduct1("schema")(s => ConfluentAvroSchemaRegistryResponseById.unapply(s).get)
  implicit val decoder: Decoder[ConfluentAvroSchemaRegistryResponseById] =
    Decoder.forProduct1("schema")(ConfluentAvroSchemaRegistryResponseById.apply)
}

case class ConfluentAvroSchemaRegistryResponseBySubjectVersion(subject: String, id: Int, version: Int, schema: String)
    extends ConfluentAvroSchemaRegistryResponse

object ConfluentAvroSchemaRegistryResponseBySubjectVersion {
  implicit val encoder: Encoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion] =
    Encoder.forProduct4("subject", "id", "version", "schema")(
      s => ConfluentAvroSchemaRegistryResponseBySubjectVersion.unapply(s).get
    )
  implicit val decoder: Decoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion] =
    Decoder.forProduct4("subject", "id", "version", "schema")(ConfluentAvroSchemaRegistryResponseBySubjectVersion.apply)
}

object ConfluentAvroSchemaRegistryResponse extends LazyLogging {
  import cats.syntax.functor._

  implicit val encoder: Encoder[ConfluentAvroSchemaRegistryResponse] = Encoder.instance {
    case byId @ ConfluentAvroSchemaRegistryResponseById(_)                                  => byId.asJson
    case bySubjectVersion @ ConfluentAvroSchemaRegistryResponseBySubjectVersion(_, _, _, _) => bySubjectVersion.asJson
    case _                                                                                  => throw new AvroCodingException("blah")
  }

  implicit val decoder: Decoder[ConfluentAvroSchemaRegistryResponse] =
    List[Decoder[ConfluentAvroSchemaRegistryResponse]](
      Decoder[ConfluentAvroSchemaRegistryResponseById].widen,
      Decoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion].widen
    ).reduceLeft(_ or _)

}
