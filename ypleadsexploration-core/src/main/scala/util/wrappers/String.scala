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
    def apply(s: String, symbols: Set[String] = Set("\"")): Clean = {
      CleanStringImpl(StringUtils.multipleReplace(s, aSet = symbols.map((_, ""))))
    }
    private[this] case class CleanStringImpl(val value: String) extends Clean
  }

  implicit def string2Clean(s: String): Clean = Clean(s)

}
