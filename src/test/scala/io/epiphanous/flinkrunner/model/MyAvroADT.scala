package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericContainer
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{AvroRuntimeException, Schema}

import java.time.Instant

sealed trait MyAvroADT extends FlinkEvent with EmbeddedAvroRecord

case class AWrapper(value: ARecord) extends MyAvroADT {
  override val $id: String      = value.a0
  override val $key: String     = $id
  override val $timestamp: Long = value.a3.toEpochMilli

  override def $recordKey: Option[String] = Some($id)
  override def $record: GenericContainer  = value
}

case class BWrapper(value: BRecord) extends MyAvroADT {
  override val $id: String                = value.b0
  override val $key: String               = $id
  override val $timestamp: Long           = value.b3.toEpochMilli
  override def $recordKey: Option[String] = Some($id)
  override def $record: GenericContainer  = value
}

case class ARecord(
    var a0: String,
    var a1: Int,
    var a2: Double,
    var a3: Instant)
    extends SpecificRecord {
  override def put(i: Int, v: Any): Unit = {
    (i, v) match {
      case (0, x: String) => this.a0 = x
      case (1, x: Int)    => this.a1 = x
      case (2, x: Double) => this.a2 = x
      case (3, x: Long)   => this.a3 = Instant.ofEpochMilli(x)
      case _              =>
        if (i < 0 || i > 3) new AvroRuntimeException("Bad index")
        else new AvroRuntimeException("Bad value")
    }
  }

  override def get(i: Int): AnyRef = i match {
    case 0 => a0.asInstanceOf[AnyRef]
    case 1 => a1.asInstanceOf[AnyRef]
    case 2 => a2.asInstanceOf[AnyRef]
    case 3 => a3.toEpochMilli.asInstanceOf[AnyRef]
    case _ => new AvroRuntimeException("Bad index")
  }

  override def getSchema: Schema = ARecord.SCHEMA$
}
object ARecord             {
  val schemaString: String =
    """
      |{
      |  "type": "record",
      |  "name": "ARecord",
      |  "namespace": "io.epiphanous.flinkrunner.model",
      |  "fields": [
      |    { "name": "a0", "type": "string" },
      |    { "name": "a1", "type": "int" },
      |    { "name": "a2", "type": "double" },
      |    { "name": "a3", "type": {"type": "long", "logicalType": "timestamp-millis" }}
      |  ]
      |}""".stripMargin
  val SCHEMA$ : Schema     = new Schema.Parser().parse(schemaString)
}

case class BRecord(
    var b0: String,
    var b1: Option[Int],
    var b2: Option[Double],
    var b3: Instant)
    extends SpecificRecord {
  override def put(i: Int, v: Any): Unit = {
    (i, v) match {
      case (0, x: String) => this.b0 = x
      case (1, x: Int)    => this.b1 = Some(x)
      case (1, _)         => this.b1 = null
      case (2, x: Double) => this.b2 = Some(x)
      case (2, _)         => this.b2 = null
      case (3, x: Long)   => this.b3 = Instant.ofEpochMilli(x)
      case _              =>
        if (i < 0 || i > 3) new AvroRuntimeException("Bad index")
        else new AvroRuntimeException("Bad value")
    }
  }

  override def get(i: Int): AnyRef = i match {
    case 0 => b0.asInstanceOf[AnyRef]
    case 1 =>
      (b1 match {
        case Some(x) => x
        case None    => null
      }).asInstanceOf[AnyRef]
    case 2 =>
      (b2 match {
        case Some(x) => x
        case None    => null
      }).asInstanceOf[AnyRef]
    case 3 => b3.toEpochMilli.asInstanceOf[AnyRef]
    case _ => new AvroRuntimeException("Bad index")
  }

  override def getSchema: Schema = BRecord.SCHEMA$
}
object BRecord             {
  val schemaString: String =
    """
      |{
      |  "type": "record",
      |  "name": "BRecord",
      |  "namespace": "io.epiphanous.flinkrunner.model",
      |  "fields": [
      |    { "name": "b0", "type": "string" },
      |    { "name": "b1", "type": ["null", "int"] },
      |    { "name": "b2", "type": ["null", "double"] },
      |    { "name": "b3", "type": {"type": "long", "logicalType": "timestamp-millis" }}
      |  ]
      |}""".stripMargin
  val SCHEMA$ : Schema     = new Schema.Parser().parse(schemaString)
}
