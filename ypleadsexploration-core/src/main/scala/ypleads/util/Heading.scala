package ypleads.util

import util.wrappers.String.{ Wrapper => StringWrapper, FromCSV => StringFromCSV }
import util.wrappers.clean.Base.CleanlyWrapped
import util.decomposable.BreakdownableOps.Breakdownable

/**
 * Definition of a 'heading' for a merchant.
 * @note An example of a heading read from a source (eg, a CSV file)
 *       is "Advertising Agencies & Activities" (\" included)
 *       A 'cleaned' heading would be == a normal cleaned string from CSV
 *
 */
object Heading {

  case class Heading(raw: String) extends StringWrapper {
    override val value = raw
  }

  implicit object HeadingBreakdownable extends Breakdownable[Heading, String] {
    // TODO: filter 'common' words? (like 'and')?
    def breakdown(aValue: Heading): Set[String] = {
      aValue.value.split("[ |&]").toSet
    }
  }

  implicit object CleanHeadingBreakdownable extends Breakdownable[CleanlyWrapped[StringFromCSV], String] {
    // TODO: filter 'common' words? (like 'and')?
    def breakdown(aValue: CleanlyWrapped[StringFromCSV]): Set[String] = {
      aValue.value.value.split("[ |&]").toSet
    }
  }

}
