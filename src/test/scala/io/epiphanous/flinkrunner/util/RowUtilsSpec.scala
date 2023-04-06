package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BWrapper, SimpleB}
import org.apache.iceberg.flink.FlinkSchemaUtil

import scala.collection.JavaConverters._

class RowUtilsSpec extends PropSpec {

  property("rowType works for avro") {
    val x = RowUtils.rowTypeOf[BWrapper]
    x shouldEqual BWrapper.getRowType
  }

  property("rowType works for non-avro") {
    val z  = RowUtils.rowTypeOf[SimpleB]
    val zz = FlinkSchemaUtil.convert(SimpleB.ICEBERG_SCHEMA)
    // Ugggh. We need this complicated comparison because FlinkSchemaUtil
    // from iceberg doesn't convert timestamps consistently
    z.getFieldNames.asScala.zipWithIndex.foreach { case (f, i) =>
      val actual   = z.getTypeAt(i)
      val expected = zz.getTypeAt(i)
      if (f == "ts") {
        actual.asSummaryString() shouldEqual "TIMESTAMP_LTZ(6) NOT NULL"
        expected.asSummaryString() shouldEqual "TIMESTAMP(6) NOT NULL"
      } else {
        actual shouldEqual expected
      }
    }
  }
}
