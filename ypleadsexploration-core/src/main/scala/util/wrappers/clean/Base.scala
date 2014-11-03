package util.wrappers.clean

import util.wrappers.Base.{ Wrapper => BaseWrapper }

object Base {

  case class CleanlyWrapped[T](value: T) extends BaseWrapper[T]

  def clean[T](aValue: T)(implicit ops: CleanableOps.Cleanable[T]): CleanlyWrapped[T] = {
    ops.clean(aValue)
  }

}

import util.wrappers.clean.Base._
object CleanableOps {
  trait Cleanable[T] {
    def clean(aValue: T): CleanlyWrapped[T]
  }

}

