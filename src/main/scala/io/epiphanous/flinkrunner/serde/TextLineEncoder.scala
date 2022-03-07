package io.epiphanous.flinkrunner.serde

abstract class TextLineEncoder[E] extends Serializable {
  def encode(event: E): String
}
