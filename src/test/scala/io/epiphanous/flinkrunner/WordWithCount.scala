package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//This line above is IMPORTANT:
//import org.apache.flink.streaming.api.scala._

/** A simple value class to hold a word and its associated count.
  * @param word
  *   the word
  * @param count
  *   a count of the number of times a word is seen in some text
  */
case class WordWithCount(word: String, count: Long) {

  /** Override the default to write a formatted word count string
    * @return
    *   String
    */
  override def toString: String = f"$count%4d $word%s"
}

object WordCountMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    // environment
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)

    // config
    val host           = "localhost"
    val port           = 9999
    val windowDuration = 5 // seconds

    // source: run `nc -l 9999` in a terminal
//    val text = env.socketTextStream(host, port, '\n')
    val text = env.fromCollection(
      Seq(
        "Here are some",
        "lines of text.",
        "We can test with",
        "these lines instead of",
        "testing actual lines from a socket connection."
      )
    )

    // transforms
    val windowCounts = text
//      .flatMap {
//        _.replaceAll("[^ a-zA-Z0-9-_]", "").toLowerCase
//          .split("\\s")
//      }
// for unit testing complex functions...
      .flatMap(new FlatMapFunction[String, String] {
        override def flatMap(line: String, out: Collector[String]): Unit =
          line
            .replaceAll("[^ a-zA-Z0-9-_]", "")
            .toLowerCase
            .split("\\s")
            .foreach(out.collect)
      })
      .filter(w => w.nonEmpty)
      .map(w => WordWithCount(w, 1))
//      .filter(w => w.word.length >= 5)
//      .filter(w => countSyllables(w.word) > 1)
//      .filter(w => w.word.startsWith("s"))
      .keyBy(w => w.word)
      .window(
        TumblingProcessingTimeWindows.of(Time.seconds(windowDuration))
      )
      .sum("count")
//      .filter(w => w.count > 1)
//      .filter(w => w.word.equals("lines"))

    /* sink */
    windowCounts.print()

    // nothing happens until we execute the job graph
    logger.debug("starting job")
    env.execute()
  }

  /** Count the number of syllables in a word. This is basically the number
    * of vowels separated by consonants in the word. The algorithm ignores
    * 'e' at the end of a word, doesn't double count multiple consecutive
    * vowels and will never return less than one as the count. This is not
    * 100% accurate. And only works, to the extent that it does, for
    * English language text.
    * @param word
    *   the word
    * @return
    *   Int number of syllables
    */
  def countSyllables(word: String): Int = {
    val s = word
      .replaceAll("([aeiouy])[aeiouy]+", "$1")
      .replaceFirst("e$", "")
      .chars()
      .filter(c => List('a', 'e', 'i', 'o', 'u', 'y').contains(c))
      .count()
      .toInt
    if (s < 1) 1 else s
  }
}
