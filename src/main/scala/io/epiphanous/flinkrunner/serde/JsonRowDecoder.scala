package io.epiphanous.flinkrunner.serde

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{
  ObjectMapper,
  ObjectReader
}

import scala.util.Try

class JsonRowDecoder[E: TypeInformation] extends RowDecoder[E] {
  val klass: Class[E] = implicitly[TypeInformation[E]].getTypeClass

  @transient
  lazy val mapper = new ObjectMapper()

  @transient
  lazy val reader: ObjectReader = mapper.readerFor(klass)

  override def decode(line: String): Try[E] =
    Try(reader.readValue(line))
}
