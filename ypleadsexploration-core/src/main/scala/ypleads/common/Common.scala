package ypleads.common

import util.{ Util => Util }
import Util.Levenshtein

object Common extends Serializable {

  object structures extends Serializable {
    // TODO: I would like the following String types to be CleanString, but saveAsParquetFile blows up on that.
    // See https://gist.github.com/ldacosta/045703aabc4aa5844c64 for a solution
    // Seems like it is making a transformation Scala types => "Hadoop types", and it needs predefined types to do that.
    case class RAMRow(accountKey: Long, keywords: String, date: String, headingId: Long, directoryId: Long, refererId: Long,
                      impressionWeight: Double, clickWeight: Double, isMobile: Boolean)

    // TODO: Strings here should be CleanString
    case class anAccount(accountKey: Long, accountId: Long, accountName: String)

  }

  object functions extends Serializable {
    // Gets rid of double-quotes around Strings.
    def cleanString(aString: String) = aString.replace("\"", "").trim

    def isCloseEnough(target: String, toMatch: String): Boolean = {
      // first, get rid of .com OR .ca at the end:
      val targetCleanedUp = {
        if (target.endsWith(".ca") || target.endsWith(".com"))
          target.substring(0, target.lastIndexOf('.'))
        else
          target
      }
      val tfTarget = targetCleanedUp.replace("-", "").replace(" ", "").replace("'", "").toUpperCase
      val tfToMatch = toMatch.replace("-", "").replace(" ", "").replace("'", "").toUpperCase
      (tfToMatch.indexOf(tfTarget) != -1) || // searched target appears somewhere in the query
        ((tfTarget.length >= 10) && (tfToMatch.indexOf(tfTarget.substring(0, tfTarget.length / 2)) != -1)) || // searched target is long and at least half of it appears in the query (ex: target = "domino's pizza", query = "dominos")
        Levenshtein.distance(tfTarget, tfToMatch) <= (tfTarget.length / 3) // edit distance
    }

  }

}
