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
    class AnAccount(account_key: Long, account_location_key: Long, account_id: String, account_name: String,
                    account_vertical: String, account_type: String, account_status: String,
                    created: java.sql.Timestamp, removed: Option[java.sql.Timestamp],
                    isremoved: Boolean, client_number: String, report_id: String, local_name: String,
                    is_demo: Boolean, is_paying: Boolean, address: String, wss_url: String, wss_product: String,
                    phone_primary: String, phone_fax: String, phone_tollfree: String, phone_secondary: String,
                    phone_mobile: String, phone_click2call: Int, tz_offset: Int, location_county: String) extends Product with Serializable {

      // syntax helper:
      private def cs(s: String): String = CleanString(account_id).toString

      def withCleanString() = {
        new AnAccount(account_key, account_location_key, account_id = cs(account_id), account_name = cs(account_name),
          account_vertical = cs(account_vertical), account_type = cs(account_type), account_status = cs(account_status),
          created, removed,
          isremoved, client_number = cs(client_number), report_id = cs(report_id), local_name = cs(local_name),
          is_demo, is_paying, address = cs(address), wss_url = cs(wss_url), wss_product = cs(wss_product),
          phone_primary = cs(phone_primary), phone_fax = cs(phone_fax), phone_tollfree = cs(phone_tollfree), phone_secondary = cs(phone_secondary),
          phone_mobile = cs(phone_mobile), phone_click2call, tz_offset, location_county = cs(location_county))
      }

      override def canEqual(that: Any): Boolean = that.isInstanceOf[AnAccount]

      override def productArity: Int = 24

      @throws(classOf[IndexOutOfBoundsException])
      override def productElement(n: Int) = n match {
        case 0 => account_key
        case 1 => account_location_key
        case 2 => account_id
        case 3 => account_name
        case 4 => account_vertical
        case 5 => account_type
        case 6 => account_status
        case 7 => isremoved
        case 8 => client_number
        case 9 => report_id
        case 10 => local_name
        case 11 => is_demo
        case 12 => is_paying
        case 13 => address
        case 14 => wss_url
        case 15 => wss_product
        case 16 => phone_primary
        case 17 => phone_fax
        case 18 => phone_tollfree
        case 19 => phone_secondary
        case 20 => phone_mobile
        case 21 => phone_click2call
        case 22 => tz_offset
        case 23 => location_county
        case _ => throw new IndexOutOfBoundsException(n.toString())
      }

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
