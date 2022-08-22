package io.epiphanous.flinkrunner.model

case class IndexColumn(
    name: String,
    position: Int,
    direction: Option[String]) {}
