package util

import java.io.{ PrintWriter, FileWriter }
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time.DateTime
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
 * Several Utilities.
 */
// TODO: since I am using this on a distributed environment, do I need to make it Serializable?
// Read on: https://spark.apache.org/docs/latest/tuning.html
object Util extends StrictLogging {

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

  object HDFS extends Serializable {
    import scala.language.postfixOps

    /**
     * Gets a "local" filesystem. Meaning that it accesses the files in a local
     * way, even if the process calling it is distributed.
     */
    private def getFileSystem: org.apache.hadoop.fs.FileSystem = {
      FileSystem.get(new Configuration())
    }

    /**
     * Computes a set of full file names contained in a folder.
     * @param folderName
     * @param recursive true if internal folders have to be scanned too
     * @return a Set of Strings
     * @note The directory that we are traversing here is actually a tree - so we should reuse
     *       the set of traversal strategies that could be applied here.
     *       For now we mark this with a TODO
     */
    def ls(folderName: String, recursive: Boolean): Set[String] = {
      try {
        val p = new Path(folderName)
        if (!getFileSystem.getFileStatus(p).isDir) {
          logger.error(s"${folderName} is *not* a folder")
          Set.empty
        } else {
          getFileSystem.listStatus(new Path(folderName)).map { status =>
            if (status.isDir) {
              if (recursive) ls(s"${folderName}${Path.SEPARATOR}${status.getPath.getName}", recursive)
              else List.empty
            } else {
              List(s"${folderName}${Path.SEPARATOR}${status.getPath.getName}")
            }
          }.flatten toSet
        }
      } catch {
        case e: Exception => {
          logger.error(s"Check of file ${folderName}: ${e.getMessage}")
          Set.empty
        }
      }
    }

    /**
     * Checks if file exists AND if it meets the criteria of being a folder or not.
     * @param fileOrDirectoryName Name of fiel or directory
     * @param asAFolder If true, checks that it is a folder.
     * @return true, if file or dir exists. false otherwise.
     *
     */
    private[util] def fileExists(fileOrDirectoryName: String, asAFolder: Boolean): Boolean = {
      val fs = getFileSystem
      val noExceptionCheck = manageOnException[Path, Boolean](fs.exists(_), e => logger.error(s"Check of file ${fileOrDirectoryName}: ${e.getMessage}")) _
      val p = new Path(fileOrDirectoryName)
      noExceptionCheck(p).getOrElse(false) && (!asAFolder || fs.getFileStatus(p).isDir)
    }

    def directoryExists(fileName: String): Boolean = {
      fileExists(fileName, asAFolder = true)
    }

    def fileExists(fileName: String): Boolean = {
      fileExists(fileName, asAFolder = false)
    }

    def mv(fileNameSrc: String, fileNameDst: String): Boolean = {
      try {
        getFileSystem.rename(new Path(fileNameSrc), new Path(fileNameDst))
      } catch {
        case e: Exception => {
          logger.error(s"Move ${fileNameSrc} => ${fileNameDst}: ${e.getMessage}")
          false
        }
      }
    }

    /**
     * Deletes a file on HDFS.
     * @param fileName The name of the file to be deleted.
     * @return true if the file no longer exists after this invocation. false otherwise.
     */
    def rm(fileName: String): Boolean = {
      if (fileExists(fileName) || directoryExists(fileName)) {
        val noExceptionDelete = manageOnException[Path, Unit](getFileSystem.delete(_, false) /* false == *not* recursive */ , e => logger.error(s"Check of file ${fileName}: ${e.getMessage}")) _
        noExceptionDelete(new Path(fileName)).isDefined
      }
      !fileExists(fileName) && !directoryExists(fileName)
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

