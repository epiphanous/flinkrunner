package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedRowType, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.utils.LogicalTypeParser
import org.apache.flink.table.types.logical.{LogicalTypeRoot, RowType}

import scala.util.Try

/** A job class to generate streaming output tables.
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class TableStreamJob[
    OUT <: ADT with EmbeddedRowType: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  def getRowType: RowType = rowTypeFromConfig.fold(
    t =>
      throw new RuntimeException(
        "failed to get row type from configuration",
        t
      ),
    rt => rt
  )

  /** Provide a [[RowType]] for the output class based on job
    * configuration. For instance,
    *
    * {{{
    * row.type = "ROW(id int not null, f1 double, f2 string not null)"
    * }}}
    *
    * If the output class of the job is unambiguous, you can use the plain
    * "row.type" configuration. If your job can have multiple output types,
    * you should use the "row.type.[simple class name]" configuration.
    *
    * Will return a Success if a valid configuration is found, or a Failure
    * if and invalid or no configuration is found.
    *
    * @return
    *   Try([[RowType]])
    */
  def rowTypeFromConfig: Try[RowType] = {
    val outClass: String =
      implicitly[TypeInformation[OUT]].getTypeClass.getSimpleName
    Try(
      runner.config
        .getStringOpt(s"row.type.$outClass")
        .orElse(runner.config.getStringOpt("row.type"))
        .map(rt => if (rt.startsWith("ROW(")) rt else s"ROW($rt)")
        .map { rt =>
          val lt =
            LogicalTypeParser.parse(rt, this.getClass.getClassLoader)
          if (lt.is(LogicalTypeRoot.ROW))
            lt.asInstanceOf[RowType]
          else
            throw new RuntimeException(
              s"row.type=$rt is an invalid flink logical ROW type definition"
            )
        }
        .getOrElse(
          throw new RuntimeException(
            s"missing row.type configuration in job ${runner.config.jobName}"
          )
        )
    )
  }

  override def sink(out: DataStream[OUT]): Unit = {
    runner.getSinkNames.foreach(name =>
      runner.addRowSink(out.map((e: OUT) => e.toRow), name, getRowType)
    )
  }
}
