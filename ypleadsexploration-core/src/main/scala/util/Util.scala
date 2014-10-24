package util

import java.io.{ PrintWriter, FileWriter }
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.StrictLogging
import util.wrappers.String.{ Clean => CleanString }

import scala.util.control.Exception._

/**
 * Several Utilities.
 */
object Util extends StrictLogging {

  /**
   * Time profiling of a call
   * @return The time a call takes, in nanoseconds.
   * @note Adapted from http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
   */
  def time[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0, result)
  }

  // TODO: I would like it better to define a StringWrapper, and then an implicit String => StringWrapper
  // Then I would do things like http://myadventuresincoding.wordpress.com/2011/04/19/scala-extending-a-built-in-class-with-implicit-conversions/
  // I don't do it NOW because of potential serialization problems with Spark. So TODO.
  object String {

    def clean(aString: String): CleanString = CleanString(aString)

    /**
     * Takes a set of string to filter from another one, and does the job.
     * @return The string filtered
     */
    def filterOccurrencesFrom(wordsToFilter: Set[String], aString: String, caseSensitive: Boolean): String = {
      // as we may have to transform the original string (dependending on the 'caseSensitive' flag),
      // we use a helper structure
      // (as at the end we *always* return the original word).
      case class OrigTransformed(orig: String, transformed: String)
      // let's transform everything based on case-sensitivity or not:
      val setToFilter = if (caseSensitive) wordsToFilter else wordsToFilter.map(_.toUpperCase)
      val targetSplit = aString.split(" ").
        map(w => OrigTransformed(orig = w, transformed = if (caseSensitive) w else w.toUpperCase))
      // and now let's do the job:
      targetSplit.filter { case OrigTransformed(_, t) => !setToFilter.contains(t) }. // keep the ones we want...
        map { case OrigTransformed(o, _) => o }. // grab the original from that...
        filter(!_.isEmpty).mkString(" ") // and rebuild a String
    }

    /**
     * Specification for a word-matcher.
     * What we want is a way to determine if two words are 'close enough'. We add the possibility
     * of having stop-words to do that match.
     */
    trait Matcher {
      val stopWords: Set[String]
      def isCloseEnough(aWord: String, anotherWord: String, verbose: Boolean = false): Boolean
    }

  }

  object Date extends Serializable {
    def allDaysStartingIn(aDate: DateTime): Stream[DateTime] = Stream.cons(aDate, allDaysStartingIn(aDate.plusDays(1)))

    /**
     * Returns all days that are "mentioned" in a given range.
     *
     * @example Days between (yesterday and today is 2, since 2 days are mentioned.
     *
     * @param fromDate Beginning of range
     * @param toDate Ending of range
     * @return a Stream with all days mentioned.
     * @note The hour/minutes/seconds mentioned in the dates returned are not especially curated. In other words,
     *       they *do not* correspond to either the beginning of the range or the ending of it.
     */
    def getAllDays(fromDate: DateTime, toDate: DateTime): Stream[DateTime] = {
      Date.allDaysStartingIn(fromDate.withHourOfDay(1).withMinuteOfHour(0).withSecondOfMinute(0)).
        takeWhile(_.isBefore(toDate.withHourOfDay(1).withMinuteOfHour(0).withSecondOfMinute(1)))
    }

  }

  /**
   * Convert an Int to a String of a given length with leading zeros to align
   * @note Grabbed from http://stackoverflow.com/questions/8131291/how-to-convert-an-int-to-a-string-of-a-given-length-with-leading-zeros-to-align
   */
  def getIntAsString(anInt: Int, minStringLength: Int): String = (s"%0${minStringLength}d").format(anInt)

  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName: String, data: String) =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName: String, textData: String) =
    using(new FileWriter(fileName, true)) {
      fileWriter =>
        using(new PrintWriter(fileWriter)) {
          printWriter => printWriter.println(textData)
        }
    }

}
