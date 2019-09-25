package io.epiphanous.flinkrunner.avro

import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._

sealed trait ConfluentAvroSchemaRegistryResponse {
  def schema: String
}

case class ConfluentAvroSchemaRegistryResponseById(schema: String) extends ConfluentAvroSchemaRegistryResponse
object ConfluentAvroSchemaRegistryResponseById {
  implicit val encoder = deriveEncoder[ConfluentAvroSchemaRegistryResponseById]
  implicit val decoder = deriveDecoder[ConfluentAvroSchemaRegistryResponseById]
}

case class ConfluentAvroSchemaRegistryResponseBySubjectVersion(subject: String, id: Int, version: Int, schema: String)
    extends ConfluentAvroSchemaRegistryResponse

object ConfluentAvroSchemaRegistryResponseBySubjectVersion {
  implicit val encoder = deriveEncoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion]
  implicit val decoder = deriveDecoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion]
}

object ConfluentAvroSchemaRegistryResponse extends LazyLogging {
  import cats.syntax.functor._

  implicit val encoder: Encoder[ConfluentAvroSchemaRegistryResponse] = Encoder.instance {
    case byId: ConfluentAvroSchemaRegistryResponseById                         => byId.asJson
    case bySubjectVersion: ConfluentAvroSchemaRegistryResponseBySubjectVersion => bySubjectVersion.asJson
    case _                                                                     => throw new AvroCodingException("Unknown schema registry response")
  }

  implicit val decoder: Decoder[ConfluentAvroSchemaRegistryResponse] =
    List[Decoder[ConfluentAvroSchemaRegistryResponse]](
      Decoder[ConfluentAvroSchemaRegistryResponseById].widen,
      Decoder[ConfluentAvroSchemaRegistryResponseBySubjectVersion].widen
    ).reduceLeft(_ or _)

}
