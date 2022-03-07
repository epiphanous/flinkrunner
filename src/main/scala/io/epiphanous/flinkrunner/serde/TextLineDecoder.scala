package io.epiphanous.flinkrunner.serde

abstract class TextLineDecoder[E] extends Serializable {
  def decode(line: String): E
}
