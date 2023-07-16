package io.epiphanous.flinkrunner.serde

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.dimafeng.testcontainers.{ContainerDef, GenericContainer}
import io.epiphanous.flinkrunner.model._
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy
import org.apache.flink.api.scala._
//import org.apache.flink.api.scala._
// ^^^^ if this line is commented and there isn't an uncommented version of this import
// add the import: optimize imports in intellij deletes it but we need it for this test

class GlueAvroRegistryKafkaRecordSerdeSpec
    extends SerdeTestFixtures
    with TestContainerForEach {

  override val containerDef: ContainerDef = GenericContainer.Def(
    dockerImage = GenericContainer.stringToDockerImage(
      "localstack/localstack-pro:latest"
    ),
    exposedPorts = Seq(4566),
    env = Map(
      "LOCALSTACK_API_KEY" -> System.getenv("LOCALSTACK_API_KEY"),
      "DNS_ADDRESSS"       -> "0"
    ),
    waitStrategy = new LogMessageWaitStrategy().withRegEx(".*Ready\\.\n")
  )

  property("AWrapper glue serde round trip") {
    withContainers { ls =>
      val test =
        SchemaRegistrySerdeTest[AWrapper, ARecord, MyAvroADT](
          defaultGlueSchemaRegConfigStr,
          Some(ls.asInstanceOf[GenericContainer])
        )
      test.init()
      test.runTest(genOne[AWrapper])
    }
  }

  property("BWrapper glue serde round trip scale") {
    withContainers { ls =>
      val test = SchemaRegistrySerdeTest[BWrapper, BRecord, MyAvroADT](
        defaultGlueSchemaRegConfigStr,
        Some(ls.asInstanceOf[GenericContainer])
      )
      test.init()
      genPop[BWrapper](100).zipWithIndex.foreach { case (b, i) =>
        if (i % 10 == 0) println(i)
        test.runTest(b)
      }
    }
  }

}
