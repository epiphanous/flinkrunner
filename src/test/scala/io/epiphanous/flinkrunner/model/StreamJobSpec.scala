package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation
import org.scalatest.Assertion

class StreamJobSpec extends PropSpec {

  def isSingleSourceOf(
      runner: FlinkRunner[_],
      outClass: String): Assertion = {
    val sources = runner.getStreamNodesInfo.filter(_.isSource)
    sources.size shouldBe 1
    sources.flatMap(_.simpleOutClass) shouldEqual List(outClass)
  }

  def isSingleSinkOf(
      runner: FlinkRunner[_],
      inClass: String): Assertion = {
    val sinks = runner.getStreamNodesInfo.filter(_.isSink)
    sinks.size shouldBe 1
    sinks.flatMap(_.simpleInClasses) shouldEqual List(inClass)
  }

  def testSingleSource(seq: Seq[SimpleA] = Seq.empty): Assertion = {
    val runner = getIdentityStreamJobRunner[SimpleA, MySimpleADT](
      executeJob = false
    )
    runner.process()
    isSingleSourceOf(runner, "SimpleA")
  }

  def testSingleSink(seq: Seq[SimpleA] = Seq.empty): Assertion = {
    val runner = getIdentityStreamJobRunner[SimpleA, MySimpleADT](
      executeJob = false
    )
    runner.process()
    isSingleSinkOf(runner, "SimpleA")
  }

  def testSingleAvroSource(seq: Seq[BWrapper] = Seq.empty): Assertion = {
    val runner =
      getIdentityAvroStreamJobRunner[BWrapper, BRecord, MyAvroADT](
        input = seq,
        executeJob = false
      )
    runner.process()
    isSingleSourceOf(runner, "BWrapper")
  }

  def testSingleAvroSink(seq: Seq[BWrapper] = Seq.empty): Assertion = {
    val runner =
      getIdentityAvroStreamJobRunner[BWrapper, BRecord, MyAvroADT](
        input = seq,
        executeJob = false
      )
    runner.process()
    isSingleSinkOf(runner, "BWrapper")
  }

  def testSingleRowSource(seq: Seq[SimpleA] = Seq.empty): Assertion = {
    val runner = getIdentityTableStreamJobRunner[SimpleA, MySimpleADT](
      input = seq,
      executeJob = false
    )
    runner.process()
    isSingleSourceOf(runner, "GenericRowData")
  }

  def testSingleRowSink(seq: Seq[SimpleA] = Seq.empty): Assertion = {
    val runner = getIdentityTableStreamJobRunner[SimpleA, MySimpleADT](
      input = seq,
      executeJob = false
    )
    runner.process()
    isSingleSinkOf(runner, "Row")
  }

  property("singleAvroSource property") {
    testSingleAvroSource()
  }

  property("seqOrSingleAvroSource property") {
    testSingleAvroSource(genPop[BWrapper]())
  }

  property("sink property") {
    testSingleSink()
    testSingleSink(genPop[SimpleA]())
    testSingleAvroSink()
    testSingleAvroSink(genPop[BWrapper]())
    testSingleRowSink()
    testSingleRowSink(genPop[SimpleA]())
  }

  property("singleSource property") {
    testSingleSource()
  }

  property("singleRowSource property") {
    testSingleRowSource()
  }

  property("filterByControlSource property") {}

  property("seqOrSingleSource property") {
    testSingleSource(genPop[SimpleA]())
  }

  property("seqOrSingleRowSource property") {
    testSingleRowSource(genPop[SimpleA]())
  }

  property("run property") {}

  property("windowedAggregation property") {}

  property("broadcastConnectedSource property") {}

  property("transform property") {}

  property("maybeSink property") {}

  property("connectedSource property") {}

}
