package util.wrappers

import util.wrappers.Base.{ Wrapper => BaseWrapper }

object String {

  trait Wrapper extends BaseWrapper[String] {
    override val value: String
    override def toString = value
  }

  /**
   * A 'clean' String is a String with no begin or end double-quotes, and trimmed.
   * @note The motivation from this stems from the need to clean the Strings saved on CSVs,
   *       that are sometimes (often?) double-quoted that way.
   */
  trait Clean extends Wrapper
  object Clean extends Serializable {
    def apply(s: String): Clean = { CleanStringImpl(s.replace("\"", "").trim) }
    private case class CleanStringImpl(val value: String) extends Clean
  }

  implicit def string2Clean(s: String): Clean = Clean(s)

}
