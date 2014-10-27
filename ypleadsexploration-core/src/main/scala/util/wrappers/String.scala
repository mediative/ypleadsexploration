package util.wrappers

import util._
import util.wrappers.Base.{ Wrapper => BaseWrapper }

object String {

  trait Wrapper extends BaseWrapper[String] {
    override val value: String
    override def toString = value
  }

  trait Clean extends Wrapper
  object Clean extends Serializable {
    def apply(s: String): Clean = { CleanStringImpl(Util.String.clean(s)) }
    private case class CleanStringImpl(val value: String) extends Clean
  }

  implicit def string2Clean(s: String): Clean = Clean(s)

}
