package io.epiphanous.flinkrunner.model.source

import com.dimafeng.testcontainers.lifecycle.and
import io.epiphanous.flinkrunner.model.{
  CheckResults,
  IcebergConfigSpec,
  MySimpleADT,
  SimpleB
}
import org.apache.flink.api.scala.createTypeInformation
import org.scalatest.Ignore

@Ignore
class IcebergSourceConfigSpec extends IcebergConfigSpec {

  override def afterContainersStart(containers: Containers): Unit = {
    super.afterContainersStart(containers)
    containers match {
      case ls and ib =>
    }
  }

  property("can read some rows") {
    withContainers { case ls and ib =>
      val data                                    = genPop[SimpleB]().sortBy(_.id)
//      println("INPUT:")
//      data.foreach(println)
      val catalog                                 = getCatalog(ls, ib)
      val schema                                  = SimpleB.ICEBERG_SCHEMA
      val table                                   = createTable(catalog, schema, "simple_b")
      val checkResults: CheckResults[MySimpleADT] =
        new CheckResults[MySimpleADT] {
          val name = "check-iceberg-read"

          override def checkOutputEvents[OUT <: MySimpleADT](
              out: List[OUT]): Unit = {
            val results = out.map(_.asInstanceOf[SimpleB]).sortBy(_.id)
//            println("OUTPUT:")
//            results.foreach(println)
            results shouldEqual data
          }
        }
      writeRowsDirectly[SimpleB](data, table, schema).fold(
        t => throw t,
        _ => readRowsAsJob[SimpleB]("simple_b", checkResults, ls, ib)
      )
    }
  }
}
