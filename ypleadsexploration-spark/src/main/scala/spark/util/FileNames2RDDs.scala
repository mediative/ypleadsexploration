package spark.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{ NoPosition, Reader }
import util.Util._

object FileNames2RDDs extends StrictLogging {

  /**
   * Produces a String out of a list of Strings.
   */
  object stringReader {
    val EOfList = "yyEndOfStringList__"
  }
  class stringReader(val theList: List[String], override val offset: Int) extends Reader[String] {
    import stringReader._

    def this(fileName: List[String]) = this(fileName, 0)

    def atEnd = first == EOfList

    def first: String = if (theList.isEmpty) EOfList else theList.head

    def rest: stringReader = new stringReader({ if (theList.isEmpty) List.empty else theList.tail }, offset + 1)

    def pos: scala.util.parsing.input.Position = NoPosition
  }

  /**
   * All parsers for an HDFS file.
   */
  object hdfsFileNameParsers extends Parsers with Serializable {
    type Elem = String

    def logErrorAndFail[T](errMsg: String, f: String => T): T = {
      logger.error(errMsg)
      f(errMsg)
    }

    /**
     * Creates a Parser that tries to parse a file using a certain delimiter.
     * In case of success
     *
     * @param delimiter
     * @param sc
     * @return
     */
    def fileParser(delimiter: String, sc: SparkContext) = new Parser[RDD[List[String]]] {
      def apply(in: Input): ParseResult[RDD[List[String]]] = {
        // TODO: this definition looks moronic, but it is related to problem described in https://gist.github.com/ldacosta/ea9d0e2ead06aafe7ca1
        val DEL1 = delimiter
        if (in.atEnd) {
          Failure("At end of Input", in)
        } else {
          val fileName = in.first
          val fs = FileSystem.get(new Configuration())
          val p = new Path(fileName)
          if (fs.exists(p) && !fs.getFileStatus(p).isDir) {
            val therdd = sc.textFile(fileName).map { line =>
              line.split(DEL1, -1).toList
            }
            if (therdd.first().length == 1) {
              // nothing was split - I will assume that the delimiter is *not* right:
              logErrorAndFail(s"File ${fileName} does not look like its lines are split by delimiter [${delimiter}]", errMsg => { Failure(errMsg, in) /* I let the "next" parser to do its job */ })
            } else {
              Success(therdd, in.rest)
            }
          } else {
            logErrorAndFail(s"File ${fileName} does not exist", errMsg => { Failure(errMsg, in.rest) /* since the file is not there, there is no point in getting the next parser to work. */ })
          }
        }
      }
    }

    /**
     * Helper parser.
     * @return
     */
    def catchAllParser(sc: SparkContext) = new Parser[RDD[List[String]]] {
      def apply(in: Input): ParseResult[RDD[List[String]]] = {
        if (in.atEnd) {
          Failure("At end of Input", in)
        } else {
          Success(sc.emptyRDD, in.rest)
        }
      }
    }

    /**
     * The runner for this parser is particular, in the sense that we would like for all file names to be
     * tried and parsed. This is in opposition to a "normal" parser, that fails as soon as a token does not conform
     * with the underlying grammar.
     * To do this we implement a "catch-all" parser: something that catches the processing of a file name when
     * all others parsers have failed, and that marks the execution as "failed" (in this case, a None in the result).
     *
     * @param fileNames
     * @param sc
     * @return
     */
    def run(fileNames: List[String], sc: SparkContext): List[RDD[List[String]]] = {
      val myParser = (fileParser(",", sc) | fileParser(";", sc) | catchAllParser(sc))*
      val input = new stringReader(fileNames)
      myParser(input) match {
        case Success(list, next) if (next.atEnd) => list
        //either an error or there is still input left
        case Failure(aMsg, _) => {
          logger.info(aMsg)
          List.empty
        }
        case _ => {
          logger.info("Parsing not complete")
          List.empty
        }
      }
    }

  }

  /**
   * Takes a file name and parses it into a RDD whose elements are the tokens found in each line.
   * So, for example, if the file is
   * luis, 1
   * another, 2
   * The result could be RDD[List(luis, 1), List(another, 2)]
   *
   * @param fileName A file name of a file in HDFS
   * @param sc a SparkContext
   * @return An RDD with the parsing result. Empty if the file does not exist, has problems, or can not be parsed.
   * @note The different options of Parsing (eg, what are the separators allowed) belong in the definition of the
   *       Parsers.
   */
  def fileName2RDD(fileName: String, sc: SparkContext): RDD[scala.List[String]] = {
    val listOfRddOpts = hdfsFileNameParsers.run(List(fileName), sc)
    if (listOfRddOpts.isEmpty) sc.emptyRDD
    else listOfRddOpts.head
  }

  /**
   * Just a little function to call and have a feedback about the results.
   * TODO: take care of it ASA I have decent unit tests
   */
  def myTest(fileName: String, sc: SparkContext) = {
    val rdd = fileName2RDD(fileName, sc)
    try {
      rdd.first()
      println(s"============> File ${fileName} ALL GOOD!!!!")
    } catch {
      case e: Exception => {
        println("RDD empty")
      }
      case e: Throwable => {
        println(s"${e.getMessage}")
      }
    }
    rdd
  }

}
// FileNames2RDDs.myTest("/source/ram/deduped-2013-07-23.csv", sc)
