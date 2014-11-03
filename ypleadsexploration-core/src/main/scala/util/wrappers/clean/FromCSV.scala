package util.wrappers.clean

import util.wrappers.String.{ FromCSV => StringFromCSV }
import util.wrappers.clean.Base.CleanlyWrapped
import util.wrappers.clean.CleanableOps.Cleanable

object FromCSV {

  /**
   * A 'clean' String from a CSV is a String from where we got rid of unwanted characters.
   * @note The motivation from this stems from the need to clean the Strings saved on CSVs,
   *       that are sometimes (often?) double-quoted
   *       (eg, a line can be ==> "a string", "another" <== instead of ==> a string, another)
   */
  object CleanFromCSVString {
    def apply(s: String)(implicit ops: CleanableOps.Cleanable[StringFromCSV]): CleanlyWrapped[StringFromCSV] = {
      ops.clean(StringFromCSV(s))
    }
    def apply(s: StringFromCSV)(implicit ops: CleanableOps.Cleanable[StringFromCSV]): CleanlyWrapped[StringFromCSV] = {
      ops.clean(s)
    }
  }

}

object FromCSVOps {

  implicit object CleanableFromCSVString extends Cleanable[StringFromCSV] {
    def clean(aValue: StringFromCSV): CleanlyWrapped[StringFromCSV] = {
      CleanlyWrapped(StringFromCSV(aValue.raw.replaceAll("\"", "")))
    }
  }

}
