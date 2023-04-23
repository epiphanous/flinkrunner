package io.epiphanous.flinkrunner.model

import org.apache.avro.reflect.AvroIgnore
import org.apache.flink.types.{Row, RowKind}

import scala.annotation.tailrec

/** Event types that are used in TableStreamJobs should implement this
  * trait. This trait endows [[FlinkEvent]] types with a `\$toRow` method,
  * to convert the underlying event type to an instance of a flink [[Row]]
  * object.
  */
trait EmbeddedRowType { this: FlinkEvent =>
  @AvroIgnore def toRow: Row = {
    productIterator.zipWithIndex.foldLeft(
      Row.withPositions($rowKind, productArity)
    ) { case (row, (value, pos)) =>
      @tailrec
      def set[T](a: T): Unit = a match {
        case o: Option[_] => set(o.getOrElse(null))
        case null         => row.setField(pos, null)
        case d: Double    => row.setField(pos, Double.box(d))
        case f: Float     => row.setField(pos, Float.box(f))
        case i: Int       => row.setField(pos, Int.box(i))
        case l: Long      => row.setField(pos, Long.box(l))
        case c: Char      => row.setField(pos, Char.box(c))
        case s: Short     => row.setField(pos, Short.box(s))
        case b: Byte      => row.setField(pos, Byte.box(b))
        case b: Boolean   => row.setField(pos, Boolean.box(b))
        case _            => row.setField(pos, a)
      }
      set(value)
      row
    }
  }

  @AvroIgnore def $rowKind: RowKind = RowKind.INSERT
}
