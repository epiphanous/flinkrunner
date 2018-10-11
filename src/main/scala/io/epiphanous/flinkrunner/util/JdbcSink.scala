package io.epiphanous.flinkrunner.util

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * A buffering jdbc sink. Expects properties contains the following settings:
  *   - `driver.name` class name of the JDBC driver
  *   - `url` the JDBC url
  *   - `username` the db username
  *   - `password` the db password
  *   - `query` used to create a prepared statement
  *   - `buffer.size` the number of elements to buffer before writing out to jdbc connection
  *
  * @param props the properties used to configure the sink
  * @tparam E the class of sink elements.
  */
class JdbcSink[E <: FlinkEvent: TypeInformation](batchFunction: AddToJdbcBatchFunction[E], props: Properties)
    extends RichSinkFunction[E]
    with CheckpointedFunction
    with LazyLogging {

  val bufferSize = props.getProperty("buffer.size").toInt
  private val pendingRows = ListBuffer.empty[E]
  private var connection: Connection = _
  private var statement: PreparedStatement = _
  private var checkpointedState: ListState[E] = _

  override def open(parameters: Configuration): Unit = {
    (for {
      _ <- Try(Class.forName(props.getProperty("driver.name")))
      conn <- Try(
        DriverManager.getConnection(
          props.getProperty("url"),
          props.getProperty("username"),
          props.getProperty("password")
        ))
      stmt <- Try(conn.prepareStatement(props.getProperty("query")))
    } yield (conn, stmt)) match {
      case Success((cn, ps)) =>
        connection = cn
        statement = ps
      case Failure(ex) => throw ex
    }
    super.open(parameters)
  }

  override def invoke(value: E): Unit = {
    pendingRows += value
    if (pendingRows.size >= bufferSize) {
      pendingRows.foreach(row => batchFunction.addToBatch(row, statement))
      Try(statement.executeBatch()) match {
        case Success(_) => pendingRows.clear()
        case Failure(ex) => throw ex
      }
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    pendingRows.foreach(checkpointedState.add)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[E](
      "buffered-elements",
      createTypeInformation[E]
    )
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored)
      for (row <- checkpointedState.get().asScala) pendingRows += row
  }
}
