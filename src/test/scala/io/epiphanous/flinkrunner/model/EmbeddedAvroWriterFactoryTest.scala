package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.local.LocalDataInputStream
import org.apache.flink.core.fs.{
  FSDataOutputStream,
  FileInputSplit,
  FileSystem,
  Path
}
import org.apache.flink.testutils.TestFileSystem

import java.io.File
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class EmbeddedAvroWriterFactoryTest extends PropSpec {

  val overwrite = FileSystem.WriteMode.OVERWRITE

  def getStream(path: String): FSDataOutputStream =
    new TestFileSystem().create(new Path(path), overwrite)

  def getWriter(path: String, isParquet: Boolean): BulkWriter[BWrapper] = {
    new EmbeddedAvroWriterFactory[BWrapper, BRecord, MyAvroADT](
      isParquet
    ).create(getStream(path))
  }

  def readAvroFile(file: String): List[BWrapper] = {
    val fileSize                   = getFileInfoView(Paths.get(file)).readAttributes().size()
    val path                       = new Path(file)
    val inputFormat                =
      new EmbeddedAvroInputFormat[BWrapper, BRecord, MyAvroADT](path)
    inputFormat.open(new FileInputSplit(1, path, 0L, fileSize, null))
    val reuse: BWrapper            = BWrapper(new BRecord())
    val pop: ArrayBuffer[BWrapper] = ArrayBuffer.empty
    var done                       = false
    while (!done) {
      val b = inputFormat.nextRecord(reuse)
      done = b == null
      if (!done) pop += b
    }
    inputFormat.close()
    pop.toList
  }

  def readParquetFile(file: String): List[BWrapper] = {
    val fileSize                   = getFileInfoView(Paths.get(file)).readAttributes().size()
    val inputFormat                =
      new EmbeddedAvroParquetInputFormat[BWrapper, BRecord, MyAvroADT]()
    val input                      = new LocalDataInputStream(new File(file))
    val reader                     = inputFormat.createReader(
      new Configuration(),
      input,
      fileSize,
      fileSize
    )
    val pop: ArrayBuffer[BWrapper] = ArrayBuffer.empty
    var done                       = false
    while (!done) {
      val b = reader.read()
      done = b == null
      if (!done) pop += b
    }
    reader.close()
    pop.toList
  }

  def writeFile(
      path: String,
      isParquet: Boolean,
      pop: List[BWrapper]): Unit = {
    val writer = getWriter(path, isParquet)
    pop.foreach(writer.addElement)
    writer.finish()
  }

  def readFile(path: String, isParquet: Boolean): List[BWrapper] =
    if (isParquet) readParquetFile(path) else readAvroFile(path)

  def getFileInfoView(path: java.nio.file.Path): BasicFileAttributeView =
    Files
      .getFileAttributeView(
        path,
        classOf[BasicFileAttributeView]
      )

  def getTempFile(isParquet: Boolean): Try[java.nio.file.Path] = {
    val pa = if (isParquet) "parquet" else "avro"
    Try(Files.createTempFile(s"$pa-test-", s".$pa"))
  }

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
