package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedRowType,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.AvroUtils.rowTypeOf
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.types.logical.RowType

import scala.util.Try

/** A stream job class to output avro records in tabular format (using
  * Flink's Row datatype). Like the TableStreamJob this job class inherits
  * from, this class also requires a flink RowType instance to define the
  * shape of the output type. This class can generate this RowType instance
  * from the avro type or take it from configuration. If implementors want
  * to use the avro inference, you need to override the `getRowType` method
  * and call this class's `getAvroRowType` method. Otherwise, flinkrunner
  * will try to generate the RowType from the sink configuration.
  *
  * This job class (like TableStreamJob) is most often used with the
  * iceberg sink, to support writing output to an iceberg data lake.
  *
  * @param runner
  *   instance of flinkrunner
  * @tparam OUT
  *   The output type (subclass of the flinkrunner ADT), which should
  *   extend the EmbeddedAvroRecord[A] and EmbeddedRowType traits
  * @tparam A
  *   The avro type
  * @tparam ADT
  *   The flinkrunner algebraic type definition
  */
abstract class AvroTableStreamJob[
    OUT <: ADT with EmbeddedAvroRecord[
      A
    ]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends TableStreamJob[OUT, ADT](runner) {

  override def getRowType: RowType =
    rowTypeFromConfig
      .orElse(rowTypeFromAvro)
      .fold(
        t =>
          throw new RuntimeException(
            "Failed to get row type from configuration or avro",
            t
          ),
        rt => rt
      )

  /** Return a RowType based on the avro schema associated with the output
    * event.
    * @return
    *   Try([[RowType]])
    */
  def rowTypeFromAvro: Try[RowType] =
    rowTypeOf(implicitly[TypeInformation[A]].getTypeClass)

}
