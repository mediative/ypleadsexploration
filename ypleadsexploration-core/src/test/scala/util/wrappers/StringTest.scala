package util.wrappers

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.wrappers.String.{ Clean => CleanString }

class StringTest extends FlatSpec with BeforeAndAfter {

  before {
  }

  after {
  }

  "Creation of a CleanString" should "do nothing when string is already clean" in {
    val aCleanString = "luisOrSomethingElse I can't think about"
    assert(CleanString(aCleanString).value == aCleanString)
  }

  it should "clean 'dirty' strings" in {
    val aDirtyString = "luis\"lala\"OrSomethingElse I can't think about"
    val aCleanString = "luislalaOrSomethingElse I can't think about"
    withClue(s"CleanString(${aDirtyString}) = ${CleanString(aDirtyString).value}") {
      assert(CleanString(aDirtyString).value == aCleanString)
    }
  }

}
