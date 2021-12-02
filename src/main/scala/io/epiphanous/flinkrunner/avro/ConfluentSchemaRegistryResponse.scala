package io.epiphanous.flinkrunner.avro

import com.typesafe.scalalogging.LazyLogging
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax._
import io.circe.{Decoder, Encoder}

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
sealed trait ConfluentSchemaRegistryResponse {
  def schema: String
}

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
case class ConfluentSchemaRegistryResponseById(schema: String)
    extends ConfluentSchemaRegistryResponse

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
object ConfluentSchemaRegistryResponseById {
  implicit val encoder = deriveEncoder[ConfluentSchemaRegistryResponseById]
  implicit val decoder = deriveDecoder[ConfluentSchemaRegistryResponseById]
}

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
case class ConfluentSchemaRegistryResponseBySubjectVersion(
    subject: String,
    id: Int,
    version: Int,
    schema: String)
    extends ConfluentSchemaRegistryResponse

object ConfluentSchemaRegistryResponseBySubjectVersion {
  implicit val encoder =
    deriveEncoder[ConfluentSchemaRegistryResponseBySubjectVersion]
  implicit val decoder =
    deriveDecoder[ConfluentSchemaRegistryResponseBySubjectVersion]
}

object ConfluentSchemaRegistryResponse extends LazyLogging {

  import cats.syntax.functor._

  implicit val encoder: Encoder[ConfluentSchemaRegistryResponse] =
    Encoder.instance {
      case byId: ConfluentSchemaRegistryResponseById                         => byId.asJson
      case bySubjectVersion: ConfluentSchemaRegistryResponseBySubjectVersion =>
        bySubjectVersion.asJson
      case _                                                                 =>
        throw new AvroCodingException("Unknown schema registry response")
    }

  implicit val decoder: Decoder[ConfluentSchemaRegistryResponse] =
    List[Decoder[ConfluentSchemaRegistryResponse]](
      Decoder[ConfluentSchemaRegistryResponseById].widen,
      Decoder[ConfluentSchemaRegistryResponseBySubjectVersion].widen
    ).reduceLeft(_ or _)

}
