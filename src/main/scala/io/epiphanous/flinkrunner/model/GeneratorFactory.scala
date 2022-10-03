package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator

@SerialVersionUID(202210031145L)
abstract class GeneratorFactory[ADT <: FlinkEvent]
    extends Serializable
    with LazyLogging {

  def getDataGenerator[E <: ADT: TypeInformation](
      generatorConfig: GeneratorConfig): DataGenerator[E] = ???

  def getAvroDataGenerator[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      generatorConfig: GeneratorConfig)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataGenerator[E] = ???

}
