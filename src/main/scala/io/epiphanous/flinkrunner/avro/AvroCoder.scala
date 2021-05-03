package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.{Decoder, Encoder}

import java.nio.ByteBuffer
import scala.util.{Failure, Success, Try}

/**
 * Uses an avro registry client to facilitate avro encoding and decoding.
 *
 * @param registry
 *   the schema registry client
 */
class AvroCoder(registry: AvroSchemaRegistryClient) {

  import AvroCoder.MAGIC_BYTE

  /**
   * Returns a byte buffer wrapped around message, checking and reading its
   * first byte matches our expected magic byte.
   *
   * @param message
   *   the byte array to wrap
   * @return
   *   the byte buffer (wrapped in a [[Try]] )
   */
  protected def getByteBuffer(message: Array[Byte]): Try[ByteBuffer] =
    Try(ByteBuffer.wrap(message)).flatMap { buffer =>
      Try(buffer.get()).flatMap[ByteBuffer] { b =>
        if (b == MAGIC_BYTE) Success(buffer)
        else
          Failure(
            new AvroCodingException(
              s"unexpected magic byte '$b' found while decoding message"
            )
          )
      }
    }

  /**
   * Avro decode a message. The message should have a leading magic byte,
   * followed by a integer ID, used to lookup the message's schema in the
   * registry.
   *
   * @param message
   *   the raw byte array
   * @tparam E
   *   the expected type of message
   * @return
   *   An instance of E (wrapped in a [[Try]] )
   */
  def decode[E: Decoder](message: Array[Byte]): Try[E] =
    getByteBuffer(message).flatMap { buffer =>
      Try(buffer.getInt()).flatMap { id =>
        println(id)
        registry.get(id).flatMap(_.decode[E](buffer))
      }
    }

  /**
   * Avro encode an event record, using the latest schema found in the
   * registry under the event record's class name (as returned by
   * event.getClass.getSimpleName).
   *
   * @param event
   *   E An event record
   * @tparam E
   *   class of the event record
   * @return
   *   A byte array (wrapped in a [[Try]] )
   */
  def encode[E: Encoder](
      event: E,
      isKey: Boolean = false): Try[Array[Byte]] =
    registry.get[E](event, isKey).flatMap(_.encode[E](event))
}

object AvroCoder {

  /**
   * A magic byte that should be the first byte read from a byte buffer
   * amenable to encoding with this library.
   */
  val MAGIC_BYTE: Byte = 0x0
}
