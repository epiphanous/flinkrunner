package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.{
  GenericContainer,
  LocalStackV2Container
}
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.RowUtils
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.types.logical.RowType
import requests.Response
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  ListObjectsV2Request
}

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class IcebergSinkConfigSpec extends IcebergConfigSpec {

  def maybeCreateTableTest(
      ls: LocalStackV2Container,
      ib: GenericContainer,
      rowType: RowType): Unit = {
    val config     = new FlinkConfig(
      Array.empty[String],
      Some(getIcebergConfig(ls, ib, "brecord", isSink = true))
    )
    val sinkConfig =
      new IcebergSinkConfig[MyAvroADT]("iceberg-sink", config)
    val table      =
      sinkConfig.maybeCreateTable(sinkConfig.getFlinkTableSchema(rowType))
    table should be a 'Success
    logger.debug(table.get.toString)
  }

  // ****************** TESTS START HERE ************************

  property("has warehouse s3 bucket") {
    withContainers { case ls and _ =>
      val s3      = getS3Client(ls)
      val buckets = s3.listBuckets().buckets().asScala.toList
      logger.debug(buckets.toString)
      buckets should have size 1
      buckets.head.name() shouldEqual bucketName
    }
  }

  property("has iceberg catalog endpoint") {
    withContainers { case _ and ib =>
      val r: Response = icebergRest(ib, "/v1/config")
      logger.debug(r.text())
      r.statusCode shouldEqual 200
    }
  }

  property("create iceberg table") {
    withContainers { case ls and ib =>
      val rowType = RowUtils.rowTypeOf[BWrapper]
      logger.debug(rowType.toString)
      maybeCreateTableTest(ls, ib, rowType)
      val s3      = getS3Client(ls)
      val r       = s3.listObjectsV2(
        ListObjectsV2Request.builder().bucket(bucketName).build()
      )
      r.contents().asScala.foreach { o =>
        println(o.key())
        println(
          s3.getObjectAsBytes(
            GetObjectRequest
              .builder()
              .bucket(bucketName)
              .key(o.key())
              .build()
          ).asString(StandardCharsets.UTF_8)
        )
      }
    }
  }

  property("can write some rows") {
    withContainers { case ls and ib =>
      val data = genPop[SimpleB]()
      writeRows(data, "simple_b", ls, ib)
      readRows[SimpleB]("simple_b", ls, ib)
    }
  }

}
