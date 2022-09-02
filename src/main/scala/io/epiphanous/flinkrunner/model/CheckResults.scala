package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.sink.SinkConfig
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
  def name: String

  /** should we write to the sink? This defaults to false since most of the
    * time this CheckResults object facilitates a check of the logic of
    * your transform method. But if you need to test the sink writing
    * itself, set this to true.
    */
  def writeToSink: Boolean = false

  /** Maximum number of records to collect from the job
    * @return
    *   Int
    */
  def collectLimit: Int = 100

  /** Return a list of test events for a given source configuration to run
    * through a mock job.
    * @tparam IN
    *   the input type
    * @param sourceConfig
    *   the source configuration
    * @return
    *   List[IN]
    */
  def getInputEvents[IN <: ADT: TypeInformation](
      sourceConfig: SourceConfig[ADT]): List[IN]

  /** Check the results of a mock run of a job.
    * @param sinkConfig
    *   the sink configuration
    * @param out
    *   a list of output events produced by the job
    * @tparam OUT
    *   the output event type
    */
  def checkOutputEvents[OUT <: ADT: TypeInformation](
      sinkConfig: SinkConfig[ADT],
      out: List[OUT]): Unit

}
