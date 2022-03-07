package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import io.circe.parser.{decode => circeDecode}

class JsonLineDecoder[E](implicit circeDecoder: Decoder[E], ev: Null <:< E)
    extends TextLineDecoder[E]
    with LazyLogging {
  override def decode(line: String): E =
    circeDecode[E](line).toOption match {
      case Some(event) => event
      case other       =>
        logger.error(s"Failed to deserialize JSON line $line")
        other.orNull
    }
}
