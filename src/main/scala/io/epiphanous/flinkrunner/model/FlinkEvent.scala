package io.epiphanous.flinkrunner.model

trait FlinkEvent extends Product with Serializable {
  def $id: String
  def $key: String
  def $timestamp: Long
  def $active: Boolean = false
}
