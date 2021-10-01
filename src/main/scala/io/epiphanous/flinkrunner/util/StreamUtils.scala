package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging

object StreamUtils extends LazyLogging {

  /**
   * A little syntactic sugar for writing stream program. This is the pipe
   * operator, ala F#.
   *
   * Assuming {{{source}}} This let's us write
   * {{{
   *   def program:DataStream[E] = source |> transform |# sink
   * }}}
   * instead of
   * {{{
   *   def program:DataStream[E] = {
   *     val result:DataStream[E] = transform(source)
   *     sink(result)
   *     result
   *   }
   * }}}
   *
   * @param v
   *   any object
   * @tparam A
   *   the type of v
   */
  implicit class Pipe[A](val v: A) extends AnyVal {
    // forward pipe op
    def |>[B](t: A => B): B = t(v)

    // side effect op
    def |#(e: A => Unit): A = {
      e(v); v
    }
  }

}
