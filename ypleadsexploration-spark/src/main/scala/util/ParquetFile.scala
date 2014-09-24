package util

import org.apache.hadoop.fs.Path
import Util._

/**
 * Definition of the differents ways a Parquet file can be represented and kept.
 *
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
case class ParquetFileExplicitMetadata(metadataFileName: String, fileNames: String*) extends ParquetFile {
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
case class ParquetFileImplicitMetadata(fileNames: String*) extends ParquetFile {
  override def ++(aParquetFile: ParquetFile): ParquetFile = {
    aParquetFile match {
      case EmptyParquetFile => this
      case p: ParquetFileImplicitMetadata => ParquetFileImplicitMetadata(p.fileNames ++ fileNames: _*)
      case p: ParquetFileExplicitMetadata => p ++ this
    }
  }
}

// An empty Parquet file.
case object EmptyParquetFile extends ParquetFile {
  override def ++(aParquetFile: ParquetFile): ParquetFile = {
    aParquetFile match {
      case EmptyParquetFile => this
      case p: ParquetFileImplicitMetadata => p
      case p: ParquetFileExplicitMetadata => p ++ this
    }
  }
}

object ParquetFile {
  // smart (?) constructors
  def empty: ParquetFile = EmptyParquetFile
  def apply(fileNames: String*) = ParquetFileImplicitMetadata(fileNames: _*)
  def apply(metadataFileName: String, fileNames: String*) = ParquetFileExplicitMetadata(metadataFileName = metadataFileName, fileNames: _*)

  // utilities
  def merge(parquetFiles: ParquetFile*): ParquetFile = {
    parquetFiles.foldLeft(empty) { case (result, toProcess) => result ++ toProcess }
  }

  def loadFromFolder(folderName: String): ParquetFile = {
    // TODO
    empty
  }

  def loadFromHDFSFolder(folderName: String): ParquetFile = {
    loadFromListOfFiles(allParquetFiles = HDFS.listOfFilesInFolder(folderName, recursive = false).filter(_ != "_SUCCESS"))
  }

  /**
   * Creates a ParquetFile structure from a list of files
   *
   * @param allParquetFiles
   * @return
   * @note scan files in folder to see if it can find a _metadata; if it does, it grabs that as the metadata.
   */
  def loadFromListOfFiles(allParquetFiles: Set[String]): ParquetFile = {
    val metadataFileNameOpt = allParquetFiles.find(_.endsWith("_metadata"))
    if (metadataFileNameOpt.isDefined) {
      ParquetFileExplicitMetadata(metadataFileNameOpt.get, allParquetFiles.filter(_ != metadataFileNameOpt.get) toSeq: _*)
    } else {
      allParquetFiles.size match {
        case 0 => empty
        case _ => ParquetFileImplicitMetadata(allParquetFiles toSeq: _*)
      }
    }
  }

  /**
   * Saves a Parquet file to an HDFS folder
   *
   * @param parquetFile
   * @param hdfsFolderName
   * @return
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
