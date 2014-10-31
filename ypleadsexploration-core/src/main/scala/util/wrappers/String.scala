package util.wrappers

import util.wrappers.Base.{ Wrapper => BaseWrapper }
import util.Util.{ String => StringUtils }

object String {

  trait Wrapper extends BaseWrapper[String] {
    override val value: String
    override def toString = value
  }

  /**
   * A 'clean' String is a String from where we got rid of unwanted characters.
   * @note The motivation from this stems from the need to clean the Strings saved on CSVs,
   *       that are sometimes (often?) double-quoted
   *       (eg, a line can be ==> "a string", "another" <== instead of ==> a string, another)
   */
  trait Clean extends Wrapper
  object Clean extends Serializable {
    def apply(s: String, r: scala.util.matching.Regex): Clean = {
      CleanStringImpl(s.replaceAll(r.toString(), ""))
    }
    def apply(s: String, thisSymbol: Char): Clean = {
      CleanStringImpl(s.replace(thisSymbol, '\0'))
    }
    private[this] case class CleanStringImpl(val value: String) extends Clean
  }

  trait FromCSV extends Clean
  object FromCSV extends Serializable {
    def apply(s: String): Clean = {
      Clean(s, r = "\"".r)
    }
  }

}
