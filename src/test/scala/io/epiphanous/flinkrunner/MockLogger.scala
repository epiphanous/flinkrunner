package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.{Logger => UnderlyingLogger}
import org.mockito.MockitoSugar

/** A simple trait to verify that an object emitted matching log messages.
  * This leverages mockito. To use it, simple extend your object with this
  * trait and invoke the didEmitLog() method on your object, passing in a
  * function that matches the log invocation you want to verify.
  */
trait MockLogger extends LazyLogging with MockitoSugar {

  val mockLogger: UnderlyingLogger = mock[UnderlyingLogger]

  when(mockLogger.isTraceEnabled).thenReturn(true)
  when(mockLogger.isDebugEnabled).thenReturn(true)
  when(mockLogger.isInfoEnabled).thenReturn(true)
  when(mockLogger.isWarnEnabled).thenReturn(true)
  when(mockLogger.isErrorEnabled).thenReturn(true)

  override protected lazy val logger: Logger = Logger(mockLogger)

  def didEmitLog(test: UnderlyingLogger => Unit): Unit = test(
    verify(mockLogger)
  )

}
