package util.wrappers

object Base {

  trait Wrapper[T] extends Serializable {
    def value: T
  }

}
