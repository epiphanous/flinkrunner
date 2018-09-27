package io.epiphanous.flinkrunner.serialization
import java.nio.charset.StandardCharsets

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.{AutoDerivation, Configuration}
import io.circe.java8.time.TimeInstances
import io.circe.parser.decode
import io.circe.syntax._
import io.epiphanous.flinkrunner.model.FlinkEvent

trait JsonConfig[E <: FlinkEvent] extends AutoDerivation with TimeInstances {
  implicit val jsonConfig: Configuration = Configuration.default.withSnakeCaseMemberNames
    .withDiscriminator("_type")

  /**
    * Deserialize a byte array into an event
    * @param bytes the byte array
    * @param decoder an implicit decoder instance
    * @return an event
    */
  def deserializeBytes(bytes: Array[Byte])(implicit decoder: Decoder[E]): E =
    deserializeString(new String(bytes, StandardCharsets.UTF_8))

  /**
    * Deserialize a raw json string into an event
    * @param jsonString the raw json string
    * @param decoder an implicit decoder instance
    * @return an event
    */
  def deserializeString(jsonString: String)(implicit decoder: Decoder[E]): E =
    decode[E](jsonString) match {
      case Right(event) => event
      case Left(error)  => throw error.fillInStackTrace()
    }

  /**
    * Serialize an event into a byte array of json
    * @param event the event
    * @param encoder an implicit encoder instance
    * @return a byte array
    */
  def serializeToBytes(event: E)(implicit encoder: Encoder[E]) = event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)

  /**
    * Serialize an event into a raw json string
    * @param event the event
    * @param pretty true to indent output string with 2 spaces
    * @param encoder implicit encoder instance
    * @return the json string
    */
  def serializeToString(event: E, pretty: Boolean = false)(implicit encoder: Encoder[E]) = {
    val json = event.asJson
    if (pretty) json.spaces2 else json.noSpaces
  }
}
