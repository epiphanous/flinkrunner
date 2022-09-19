package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.util.{Failure, Success, Try}

abstract class EmbeddedAvroRowDecoder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](implicit fromKV: (EmbeddedAvroRecordInfo[A]) => E)
    extends RowDecoder[E] {

  val decoder: RowDecoder[A]

  override def tryDecode(line: String): Try[E] = {
    logger.debug(s"trying to decode $line")
    decoder.tryDecode(line) match {
      case Failure(err)   =>
        logger.error(err.getMessage)
        Failure(err)
      case Success(value) =>
        val info = EmbeddedAvroRecordInfo(value)
        val e    = fromKV(info)
        Success(e)
    }
  }
}
