package io.epiphanous.flinkrunner.model

/** An interface for a class that generates input events and test output
  * events of flink jobs.
  *
  * @tparam ADT
  *   The flinkrunner algebraic data type
  */
trait CheckResults[ADT <: FlinkEvent] {

  /** a name for this check configuration */
  val name: String

  /** should we write to the sink? This defaults to false since most of the
    * time this CheckResults object facilitates a check of the logic of
    * your transform method. But if you need to test the sink writing
    * itself, set this to true.
    */
  val writeToSink: Boolean = false

  /** Maximum number of records to collect from the job
    * @return
    *   Int
    */
  val collectLimit: Int = 100

  /** a list of test events for a given source configuration to run through
    * a mock job.
    */
  def getInputEvents[IN <: ADT](sourceName: String): List[IN] = List.empty

  /** Check the results of a mock run of a job.
    * @param out
    *   a list of output events produced by the job
    */
  def checkOutputEvents[OUT <: ADT](out: List[OUT]): Unit

}
