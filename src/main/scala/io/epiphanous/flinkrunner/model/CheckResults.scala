package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.source.SourceConfig
import org.apache.flink.api.common.typeinfo.TypeInformation

/** An interface for a class that generates input events and test output
  * events of flink jobs.
  *
  * @tparam ADT
  *   The flinkrunner algebraic data type
  */
trait CheckResults[ADT <: FlinkEvent] {

  /** a name for this check configuration */
  def name: String = this.getClass.getSimpleName

  /** Return a list of test events to run through a mock job.
    * @tparam IN
    *   the input type
    * @return
    *   List[IN]
    */
  def getInputEvents[IN <: ADT: TypeInformation](
      sourceConfig: SourceConfig[ADT]): List[IN]

  /** Check the results of a mock run of a job.
    * @param out
    *   the list of output events
    * @tparam OUT
    *   the ourput event type
    */
  def checkOutputEvents[OUT <: ADT: TypeInformation](out: List[OUT]): Unit

}
