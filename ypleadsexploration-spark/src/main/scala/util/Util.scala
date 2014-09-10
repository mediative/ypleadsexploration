package util

import java.io.{ PrintWriter, FileWriter }
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time.DateTime
import scala.language.reflectiveCalls

/**
 * Several Utilities.
 */
// TODO: since I am using this on a distributed environment, do I need to make it Serializable?
// Read on: https://spark.apache.org/docs/latest/tuning.html
object Util {

  /**
   * Protects the call on a function for the case where an Exception is thrown.
   *
   * @param f Function to be called
   * @param excManager What to do in case of an Exception
   * @return an Option with a result.
   */
  private[util] def manageOnException[T, U](f: T => U, excManager: Throwable => Unit)(p: T): Option[U] = {
    try {
      Some(f(p))
    } catch {
      case e: Exception => {
        excManager(e)
        None
      }
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

  object HDFS extends Serializable {

    /**
     * Gets a "local" filesystem. Meaning that it accesses the files in a local
     * way, even if the process calling it is distributed.
     */
    private def getFileSystem: org.apache.hadoop.fs.FileSystem = {
      FileSystem.get(new Configuration())
    }

    def fileExists(fileName: String): Boolean = {
      // TODO: replace 'println' with some kind of logger (maybe scalaz's?)
      val noExceptionCheck = manageOnException[Path, Boolean](getFileSystem.exists(_), e => println(s"Check of file ${fileName}: ${e.getMessage}")) _
      noExceptionCheck(new Path(fileName)).getOrElse(false)
    }

    def deleteFile(fileName: String): Boolean = {
      // TODO: replace 'println' with some kind of logger (maybe scalaz's?)
      val noExceptionDelete = manageOnException[Path, Unit](getFileSystem.delete(_, false) /* false == *not* recursive */ , e => println(s"Check of file ${fileName}: ${e.getMessage}")) _
      noExceptionDelete(new Path(fileName)).isDefined
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

  import scala.math._

  // FIXME: replace this code using https://github.com/rockymadden/stringmetric
  object Levenshtein extends Serializable {
    def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)
    def distance(s1: String, s2: String) = {
      val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

      for (j <- 1 to s2.length; i <- 1 to s1.length)
        dist(j)(i) = {
          if (s2.charAt(j - 1) == s1.charAt(i - 1)) dist(j - 1)(i - 1)
          else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
        }

      dist(s2.length)(s1.length)
    }
  }

}

