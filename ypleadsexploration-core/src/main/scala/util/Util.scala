package util

import java.io.{ PrintWriter, FileWriter }
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
 * Several Utilities.
 */
object Util extends StrictLogging {

  // TODO: I would like it better to define a StringWrapper, and then an implicit String => StringWrapper
  // Then I would do things like http://myadventuresincoding.wordpress.com/2011/04/19/scala-extending-a-built-in-class-with-implicit-conversions/
  // I don't do it NOW because of potential serialization problems with Spark. So TODO.
  object String {
    /**
     * Splits a string, using a specified delimiter.
     *
     * @note Makes some half-sophisticated things, like:
     *       input = "hi,,,," => List("hi","","","","")
     */
    def completeSplit(s: String, del: String): List[String] =
      {
        val EMPTY_SYMBOL = "yy_ee"
        val newS = { s.replace(s"${del}${del}", s"${del}${EMPTY_SYMBOL}${del}") } ++ (if (s.endsWith(del)) s"${EMPTY_SYMBOL}${del}" else "")
        newS.split(del). // do the actual splitting, and...
          map(x => if (x == EMPTY_SYMBOL) "" else x).toList // ...then clean up result
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
