package spark.util

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

}

