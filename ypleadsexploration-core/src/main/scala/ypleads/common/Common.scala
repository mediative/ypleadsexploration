package ypleads.common

import util.wrappers.String.{ FromCSV => CleanCSVString }

object Common extends Serializable {

  object structures extends Serializable {
    // TODO: I would like the following String types to be CleanString, but saveAsParquetFile blows up on that.
    // Look here for a reason why: https://gist.github.com/ldacosta/1cbd0f1387e7c92fea42
    // TODO: actually put here *all* fields from DB, with *same* names
    case class RAMRow(accountKey: Long, keywords: String, date: String, headingId: Long, directoryId: Long, refererId: Long,
                      impressionWeight: Double, clickWeight: Double, isMobile: Boolean) {
      def withCleanString() = this.copy(keywords = CleanCSVString(keywords).toString, date = CleanCSVString(date).toString)
    }

    // TODO: Strings here should be CleanString
    // Look here for a reason why it is not: https://gist.github.com/ldacosta/1cbd0f1387e7c92fea42
    // TODO: actually put here *all* fields from DB, with *same* names
    case class AnAccount(accountKey: Long, accountId: Long, accountName: String) {
      def withCleanString() = this.copy(accountName = CleanCSVString(accountName).toString)
    }

  }

  object functions extends Serializable {

  }

}
