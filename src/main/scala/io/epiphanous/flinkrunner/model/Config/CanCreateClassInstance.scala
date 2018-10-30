package io.epiphanous.flinkrunner.model.Config

trait CanCreateClassInstance {
  def classInstanceOf[T](className: String): T = Class.forName(className).newInstance().asInstanceOf[T]
}
