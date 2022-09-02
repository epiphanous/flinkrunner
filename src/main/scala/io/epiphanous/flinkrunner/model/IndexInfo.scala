package io.epiphanous.flinkrunner.model

case class IndexInfo(
    name: String,
    unique: Boolean,
    columns: List[IndexColumn]) {}
