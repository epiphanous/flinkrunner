package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

class EmbeddedAvroDelimitedRowDecoder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](
    delimitedConfig: DelimitedConfig = DelimitedConfig.CSV)(implicit
    fromKV: (EmbeddedAvroRecordInfo[A]) => E)
    extends EmbeddedAvroRowDecoder[E, A, ADT] {

  override val decoder = new DelimitedRowDecoder[A](delimitedConfig)

}
