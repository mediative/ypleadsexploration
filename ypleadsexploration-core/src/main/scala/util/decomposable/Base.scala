package util.decomposable

object Base {

  def breakdown[T, R](aValue: T)(implicit ops: BreakdownableOps.Breakdownable[T, R]): Set[R] = {
    ops.breakdown(aValue)
  }

}

object BreakdownableOps {
  trait Breakdownable[T, R] {
    // TODO: output a *weighted* Set? (eg, weight ~ number of object type R in object type T)
    def breakdown(aValue: T): Set[R]
  }

  implicit object StringBreakdownable extends Breakdownable[String, String] {
    def breakdown(aValue: String): Set[String] = aValue.split(" ").toSet
  }

}

