package util.wrappers.clean

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.wrappers.clean.Base._
import util.wrappers.clean.FromCSV.CleanFromCSVString
import util.wrappers.String.{ FromCSV => StringFromCSV }

class FromCSVTest extends FlatSpec with BeforeAndAfter {
  import util.wrappers.clean.FromCSVOps._ // brings implicits into scope

  before {
  }

  after {
  }

  "Creation of a clean string from CSV" should "do nothing when string is already clean" in {
    val aCleanString = "luisOrSomethingElse I can't think about"
    assert(CleanFromCSVString(aCleanString).value.value == aCleanString)
  }

  it should "do nothing when string is already clean (2)" in {
    val aCleanString = "luisOrSomethingElse I can't think about"
    assert(clean(StringFromCSV(aCleanString)).value.value == aCleanString)
  }

  it should "clean 'dirty' strings" in {
    val aDirtyString = "luis\"lala\"OrSomethingElse I can't think about"
    val aCleanString = "luislalaOrSomethingElse I can't think about"
    val result = CleanFromCSVString(aDirtyString).value.value
    withClue(s"CleanString(${aDirtyString}) = ${result}") {
      assert(result == aCleanString)
    }
  }

}
