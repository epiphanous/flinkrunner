package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.model.{MySimpleADT, SimpleA, SimpleB}
import org.apache.flink.api.scala.createTypeInformation

import java.time.Duration

class FlinkRunnerSpec extends PropSpec {

  property("getSocketSerializationSchema property") {}

  property("getElasticsearchSinkEncoder property") {}

  property("getRMQSerializationSchema property") {}

  property("fromKinesis property") {}

  property("getRMQDeserializationSchema property") {}

  property("mockSources property") {}

  property("maybeAssignTimestampsAndWatermarks property") {}

  property("getFileSourceBulkFormat property") {}

  property("getFileSink property") {}

  property("getWatermarkStrategy property") {}

  property("handleResults property") {}

  property("showJobHelp property") {}

  property("invoke property") {}

  property("getFileSinkEncoder property") {}

  property("getCollectionSource property") {}

  property("process property") {}

  property("getJdbcSinkStatementBuilder property") {}

  property("boundedLatenessWatermarks property") {
    val blwm  =
      mySimpleFlinkRunner.boundedLatenessWatermarks[SimpleA]("test-stream")
    blwm.maxAllowedLateness shouldEqual Duration.ofMinutes(5)
    blwm.streamID shouldEqual "test-stream"
    val blwm2 =
      getRunner[MySimpleADT](Array("nothing"), Some("max.lateness = 10m"))
        .boundedLatenessWatermarks[SimpleB]("another-stream")
    blwm2.maxAllowedLateness shouldEqual (Duration.ofMinutes(10))
    blwm2.streamID shouldEqual "another-stream"
  }

  property("getRabbitMQSink property") {}

  property("fromSocket property") {}

  property("getSocketSink property") {}

  property("fromFile property") {}

  property("getBucketAssigner property") {}

  property("ascendingTimestampsWatermarks property") {}

  property("getKafkaSink property") {}

  property("getKafkaRecordSerializationSchema property") {}

  property("getBulkFileWriter property") {}

  property("getCassandraSink property") {}

  property("fromCollection property") {}

  property("getElasticsearchSink property") {}

  property("getRMQSinkPublishOptions property") {}

  property("fromSource property") {}

  property("getSocketDeserializationSchema property") {}

  property("getSourceFilePath property") {}

  property("fromRabbitMQ property") {}

  property("getKinesisSerializationSchema property") {}

  property("getJdbcSink property") {}

  property("boundedOutOfOrderWatermarks property") {}

  property("getKinesisSink property") {}

  property("getFileSourceStreamFormat property") {}

  property("fromKafka property") {}

  property("toSink property") {}

  property("getKafkaRecordDeserializationSchema property") {}

  property("getKinesisDeserializationSchema property") {}

}
