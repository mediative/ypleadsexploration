package spark.util

import org.apache.hadoop.fs.Path
import Util._
import spark.util.parquet.Representations.Spark
import spark.util.parquet.{ ctx, ParquetMetadataSemantics }

/**
 * Definition of the differents ways a Parquet file can be represented and kept.
 *
 * @note Here we define a Monoid on ParquetFile's. TODO: Tie this with Monoid's typeclass in scalaz.
 * @note All these constructors are private because I want the construction to happen through the smart constructors
 *       present in the companion object
 */
sealed trait ParquetFile {
  // Composition of Parquet files
  def ++(aParquetFile: ParquetFile): ParquetFile

  // Saves all different parts to an HDFS folder
  def saveToHDFSFolder(hdfsFolderName: String): Boolean = {
    ParquetFile.saveToHDFSFolder(this, hdfsFolderName)
  }
}

// A Parquet File can be a bunch of individual files, accompanied with a metadata file
private case class ParquetFileExplicitMetadata(metadataFileName: String, fileNames: String*) extends ParquetFile {
  override def ++(aParquetFile: ParquetFile): ParquetFile = {
    aParquetFile match {
      case EmptyParquetFile => this
      case p: ParquetFileImplicitMetadata => ParquetFileExplicitMetadata(metadataFileName, p.fileNames ++ fileNames: _*)
      case p: ParquetFileExplicitMetadata =>
        ParquetFileExplicitMetadata(metadataFileName, fileNames ++ p.fileNames: _*)
    }
  }

}

// A Parquet File can be a bunch of individual files, with no explicit metadata.
// In that case the metadata lives in the individual files
private case class ParquetFileImplicitMetadata(fileNames: String*) extends ParquetFile {
  override def ++(aParquetFile: ParquetFile): ParquetFile = {
    aParquetFile match {
      case EmptyParquetFile => this
      case p: ParquetFileImplicitMetadata => ParquetFileImplicitMetadata(p.fileNames ++ fileNames: _*)
      case p: ParquetFileExplicitMetadata => p ++ this
    }
  }
}

// An empty Parquet file.
private case object EmptyParquetFile extends ParquetFile {
  override def ++(aParquetFile: ParquetFile): ParquetFile = {
    aParquetFile match {
      case EmptyParquetFile => this
      case p: ParquetFileImplicitMetadata => p
      case p: ParquetFileExplicitMetadata => p ++ this
    }
  }
}

object ParquetFile {
  // smart constructors
  def apply(): ParquetFile = EmptyParquetFile
  def apply[repr[String] <: ctx[String]](wrappedFileNames: repr[String]*)(implicit p: ParquetMetadataSemantics[repr]): ParquetFile = {
    val fileNames = wrappedFileNames.map(_.value)
    wrappedFileNames.find(p.isMetadataFileName(_)).map {
      metadataFileNameInCtx =>
        val s = metadataFileNameInCtx.value
        ParquetFileExplicitMetadata(metadataFileName = s, fileNames.filter(_ != s): _*)
    }.getOrElse(ParquetFileImplicitMetadata(fileNames: _*))
  }

  // utilities
  def merge(parquetFiles: ParquetFile*): ParquetFile = {
    parquetFiles.foldLeft(EmptyParquetFile: ParquetFile) { case (result, toProcess) => result ++ toProcess }
  }

  def loadFromFolder(folderName: String): ParquetFile = {
    // TODO
    ???
  }

  /**
   * Interprets the information found in an HDFS folder as the different parts of a Parquet file.
   * @param folderName The folder in question.
   * @return a parquet File
   * @note The motivation for this utility is the following:
   *       From Spark we can execute a "map-reduce" job that will transform an unstructured text file
   *       into a Parquet file. In that case, the result will be written into a directory with differents
   *       parts in it:
   *       (*) a file containing the status of the job (called typically _SUCCESS if all went well)
   *       (*) a file with the metadata of all the Parquet files contained in the folder. This file is called _metadata
   *       (*) a bunch of files, with names part-r-* (example: part-r-0000, part-r-0001, etc).
   */
  def loadFromHDFSFolder(folderName: String): ParquetFile = {
    import spark.util.parquet.Representations._

    val wrappedFilesInFolder = HDFS.ls(folderName, recursive = false).filter(_ != "_SUCCESS").map(Spark(_)).toSeq
    apply(wrappedFileNames = wrappedFilesInFolder: _*)
  }

  /**
   * Saves a Parquet file to an HDFS folder
   *
   * @param parquetFile
   * @param hdfsFolderName
   * @return
   * @note The Rollback functionality has to be implemented here
   *       This has to be defined in a generic interface in terms of primitives, then the recovery strategy
   *       is defined based on function application and composition of those primitives.
   *       The specifics for 'HDFS' domain would then be contained in an object which defined semantics for
   *       those primitives for the HDFS domain.
   *       Doing it like this ensures that if we want to use the same recovery strategy in another setting,
   *       we only need to define the primitive semantics. not only that this also allows to come up with alternative
   *       recover strategies (for different tradeoffs)
   *
   *       Now what primitives we need for generic recovery is something which needs a little thought.
   */
  def saveToHDFSFolder(parquetFile: ParquetFile, hdfsFolderName: String): Boolean = {
    def moveFileNames(fileNames: Seq[String]): Boolean = {
      // TODO: implement rollback!!!!
      fileNames.foreach { fileName =>
        val fileNameNoDir = (new Path(fileName)).getName
        HDFS.mv(fileName, s"${hdfsFolderName}${Path.SEPARATOR}${fileNameNoDir}")
      }
      true
    }
    if (!HDFS.directoryExists(hdfsFolderName)) {
      println(s"HDFS folder ${hdfsFolderName} does not exist")
      false
    } else {
      parquetFile match {
        case EmptyParquetFile => true
        case p: ParquetFileExplicitMetadata => // need to save metadata
          HDFS.mv(p.metadataFileName, s"${hdfsFolderName}${Path.SEPARATOR}${(new Path(p.metadataFileName)).getName}")
          moveFileNames(p.fileNames)
        case p: ParquetFileImplicitMetadata =>
          moveFileNames(p.fileNames)
      }
    }
  }

}

