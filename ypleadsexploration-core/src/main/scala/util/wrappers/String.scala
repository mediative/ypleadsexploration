package util.wrappers

import util.wrappers.Base.{ Wrapper => BaseWrapper }

object String {

  trait Wrapper extends BaseWrapper[String] {
    override def toString = value
  }

  case class FromCSV(raw: String) extends Wrapper {
    override val value = raw
  }

}

