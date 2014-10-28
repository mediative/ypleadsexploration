package ypleads.common

import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import util.wrappers.String.{ Clean => CleanString }

object Common extends Serializable {

  object structures extends Serializable {
    // TODO: I would like the following String types to be CleanString, but saveAsParquetFile blows up on that.
    // Look here for a reason why: https://gist.github.com/ldacosta/1cbd0f1387e7c92fea42
    // TODO: actually put here *all* fields from DB, with *same* names
    case class RAMRow(accountKey: Long, keywords: String, date: String, headingId: Long, directoryId: Long, refererId: Long,
                      impressionWeight: Double, clickWeight: Double, isMobile: Boolean) {
      def withCleanString() = this.copy(keywords = CleanString(keywords).toString, date = CleanString(date).toString)
    }

    // TODO: Strings here should be CleanString
    // Look here for a reason why it is not: https://gist.github.com/ldacosta/1cbd0f1387e7c92fea42
    // TODO: actually put here *all* fields from DB, with *same* names
    case class AnAccount(accountKey: Long, accountId: Long, accountName: String) {
      def withCleanString() = this.copy(accountName = CleanString(accountName).toString)
    }

  }

  object functions extends Serializable {

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
        LevenshteinMetric.compare(tfTarget, tfToMatch).getOrElse(Int.MaxValue) <= (tfTarget.length / 3) // edit distance
    }

  }

}
