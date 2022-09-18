package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

import java.nio.file.Files
import scala.util.Try

class EmbeddedAvroWriterFactoryTest
    extends PropSpec
    with AvroFileTestUtils {

  def doWriteTest(isParquet: Boolean): Unit = {
    getTempFile(isParquet).map { path =>
      Try {
        val fileInfoView = getFileInfoView(path)
        val before       = fileInfoView.readAttributes()
        writeFile(path.toString, isParquet, genPop[BWrapper]())
        val after        = fileInfoView.readAttributes()
        after.size() should be > before.size()
      } should be a 'success
      Try(Files.delete(path)) should be a 'success
    } should be a 'success
  }

  def doRoundTripTest(isParquet: Boolean): Unit = {
    val pop = genPop[BWrapper](10)
    getTempFile(isParquet).map { path =>
      Try {
        val file   = path.toString
        writeFile(file, isParquet, pop)
        val result = readFile(file, isParquet)
        result shouldEqual pop
      } should be a 'success
      Try(Files.delete(path)) should be a 'success
    } should be a 'success
  }

  property("avroWriterFactory writes avro property") {
    doWriteTest(false)
  }

  property("avroWriterFactory writes parquet property") {
    doWriteTest(true)
  }

  property("round trip avro file property") {
    doRoundTripTest(false)
  }

  property("round trip parquet file property") {
    doRoundTripTest(true)
  }

}
