package ypleads

import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import com.typesafe.scalalogging.slf4j.StrictLogging
import util.Util.String.{ Matcher => StringMatcher }
import util.Util._
import scala.util.control.Exception._

class Query2BusinessMatcher(val stopWords: Set[String]) extends StringMatcher with StrictLogging {
  import Query2BusinessMatcher._

  def filterStopWordsFrom(aString: String): String = String.filterOccurrencesFrom(stopWords, aString, caseSensitive = false)

  def isCloseEnough(target: String, toMatch: String): Boolean = {
    // first, get rid of .com OR .ca at the end:
    val targetCleanedUp = {
      if (target.endsWith(".ca") || target.endsWith(".com"))
        target.substring(0, target.lastIndexOf('.'))
      else
        target
    }
    val ucTarget = targetCleanedUp.replace("-", " ").toUpperCase
    val ucToMatch = toMatch.toUpperCase

    logger.debug(s"target: ${target}, cleanedTarget = ${ucTarget}, toMatch = ${toMatch}, cleanedToMatch = ${ucToMatch}")
    if (ucTarget == ucToMatch) {
      true
    } else {
      val tfTarget = filterStopWordsFrom(ucTarget)
      val tfToMatch = filterStopWordsFrom(ucToMatch)
      logger.debug(s"AFTER STOP WORDS CLEANING ==> target = ${tfTarget}, toMatch = ${tfToMatch}")
      if (tfTarget.isEmpty || tfToMatch.isEmpty) {
        false
      } else {
        val bTargetInToMatch = (tfToMatch.indexOf(tfTarget) != -1) // searched target appears somewhere in the query
        val levDistance = LevenshteinMetric.compare(tfTarget, tfToMatch).getOrElse(Int.MaxValue)
        val levThreshold = math.ceil((tfTarget.length: Double) / 3)
        val levMatch = levDistance <= levThreshold // edit distance
        logger.debug(s"bTargetInToMatch = ${bTargetInToMatch}, levMatch = ${levMatch} (lev distance = ${levDistance}, threshold = ${levThreshold}})")
        bTargetInToMatch || levMatch
      }
    }
  }
}

object Query2BusinessMatcher {

  import util.wrappers.String.{ Clean => CleanString }

  /**
   * Cleans a string, following the specifics needs for this matcher.
   * @note The headings and the synonyms will be read from (CSV) files where they may be surrounderd by double-quotes,
   *       and will be sometimes part of a larger term (eg, "Boats & Sails"). Since we will want to separate the 2,
   *       the '&' in the middle is not important and we can get rid of.
   */
  private def cleanString(aString: String): String = CleanString(aString, symbols = Set("\"", "&")).value

  // TODO: all this thing down here should be subsumed using Parsers Combinators.
  /**
   * Loads the (English) headings from a set of lines.
   * Each line is supposed to have an id plus a name for  the header, separated by tabs.
   * If that structure is not present, we ignore that specific line.
   * @param fileLines A bunch of lines of text
   * @return The set of English headings extracted from those lines
   */
  private[ypleads] def loadEnglishHeadings(fileLines: Set[String]): Set[String] = {
    fileLines.
      map(s => s.split("\t")).
      flatMap { a =>
        val Array(heading_id_s, heading_name, _) = a
        if ((catching(classOf[Exception]) opt cleanString(heading_id_s.trim).toLong).isDefined)
          Some(cleanString(heading_name))
        else
          None
      } toSet
  }

  private[ypleads] def loadEnglishHeadings(fileName: String): Set[String] = {
    loadEnglishHeadings(fileLines = scala.io.Source.fromFile(fileName).getLines().toSet)
  }

  /**
   * Loads the (English) synonyms for the headings.
   * Each line is supposed to have the synonym, a language and a specific code. A line
   * will be considered invalid if it does not respect that standard.
   * @param fileLines
   * @return
   */
  private[ypleads] def loadSynonymsForEnglishHeadings(fileLines: Set[String]): Set[String] = {
    fileLines.
      map(s => s.split(",")).
      flatMap { a =>
        val Array(synonym, lang, headingCode, _*) = a
        if ((catching(classOf[Exception]) opt cleanString(headingCode.trim).toLong).isDefined && lang.toUpperCase().equals("EN"))
          Some(cleanString(synonym))
        else
          None
      } toSet
  }

  private[ypleads] def loadSynonymsForEnglishHeadings(fileName: String): Set[String] = {
    loadSynonymsForEnglishHeadings(fileLines = scala.io.Source.fromFile(fileName).getLines().toSet)
  }

  /**
   * Loads the stop-words, consisting in the (English) headings plus their synonyms, if present.
   * @return
   */
  private def loadEnglishStopWords(headingsFileName: String, synonymsFileName: Option[String]): Set[String] = {
    // I split the expressions in individual words:
    val allWords =
      loadEnglishHeadings(headingsFileName).map(_.trim().replaceAll(" +", " ").split(" ").toList).flatten
    (if (synonymsFileName.isDefined) {
      allWords ++ loadSynonymsForEnglishHeadings(synonymsFileName.get).map(_.trim().replaceAll(" +", " ").split(" ").toList).flatten
    } else {
      allWords
    })
  }

  /**
   * Creates an instance with specific stop-words.
   */
  def apply(headingsFileName: String, synonymsFileName: Option[String]) = {
    new Query2BusinessMatcher(stopWords = loadEnglishStopWords(headingsFileName, synonymsFileName))
  }
}

