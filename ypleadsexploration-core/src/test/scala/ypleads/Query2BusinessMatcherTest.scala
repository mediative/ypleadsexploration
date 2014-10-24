package ypleads

import java.io.BufferedReader

import org.scalatest.{ BeforeAndAfter, FlatSpec }
import scala.io.Source
import scala.util.control.Exception._

class Query2BusinessMatcherTest extends FlatSpec with BeforeAndAfter {

  val stringMatcher = new Query2BusinessMatcher(stopWords =
    {
      val headAndNamesResourceName = "headingsIdAndNames.txt"
      (catching(classOf[Exception]) opt Source.fromURL(getClass.getResource(s"/${headAndNamesResourceName}")).bufferedReader()) match {
        case None => {
          withClue(s"${headAndNamesResourceName} does not exist in resources") { assert(false) }
          Set.empty
        }
        case Some(bR) => {
          info(s"Loaded headings from resources file ${headAndNamesResourceName} as stop words")
          val headings = Query2BusinessMatcher.loadEnglishHeadings(Stream.continually(bR.readLine()).takeWhile(_ != null).toSet)
          val headingsSynonyms =
            {
              val synonymsFileName = "synonyms.csv"
              (catching(classOf[Exception]) opt Source.fromURL(getClass.getResource(s"/${synonymsFileName}")).bufferedReader()) match {
                case None => {
                  info(s"Synonyms file ${synonymsFileName} does not exist in resources (or is invalid)")
                  Set.empty[String]
                }
                case Some(synonymLinesBR) => {
                  info(s"Loaded synonyms file ${synonymsFileName} from resources")
                  Query2BusinessMatcher.loadSynonymsForEnglishHeadings(Stream.continually(synonymLinesBR.readLine()).takeWhile(_ != null).toSet)
                }
              }
            }
          headings ++ headingsSynonyms
        }
      }
    })

  before {
  }

  after {

  }

  val pizzaWords = Set("PIZZA")
  val DOMINOS_PIZZA = "Domino's Pizza"
  s"Filtering stopwords = ${pizzaWords.mkString(start = "{", sep = ",", end = "}")} from ${DOMINOS_PIZZA}" should "yield 'Domino's" in {
    val qMatcher = new Query2BusinessMatcher(stopWords = pizzaWords)
    assert(qMatcher.filterStopWordsFrom(DOMINOS_PIZZA) == "Domino's")
  }

  s"Strings close enough to '${DOMINOS_PIZZA}'" should "include 'Dominos P'" in {
    assert(stringMatcher.isCloseEnough(toMatch = DOMINOS_PIZZA, target = "Dominos P"))
  }

  it should "include 'Dominos'" in {
    assert(stringMatcher.isCloseEnough(toMatch = DOMINOS_PIZZA, target = "Dominos"))
  }

  val SPINZ_LAUNDRIES = "Spinz Coin Laundries"
  s"'${SPINZ_LAUNDRIES}'" should "be close enough to some strategic words" in {
    val results =
      List("spinz coin laundries",
        "spinz-coin-laundries").map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = SPINZ_LAUNDRIES)) }
    results.foreach {
      case (anExpression, matched) =>
        if (!matched) info(s"'${SPINZ_LAUNDRIES}' DID NOT match '${anExpression}', but it should have!")
    }
    assert(results.forall { case (_, matched) => matched })
  }

  it should "NOT be close to some silly words" in {
    val results =
      List("coin laundry",
        "laundries",
        "laundries-self-service",
        "laundries - self-service",
        "laundromat",
        "laundromats",
        "laundrymat",
        "bubbles-wash-fold-laundry-service",
        "clean cycle",
        "laundromats blue").map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = SPINZ_LAUNDRIES)) }
    results.foreach {
      case (anExpression, matched) =>
        if (matched) info(s"'${SPINZ_LAUNDRIES}' matched '${anExpression}', but it shouldn't have!")
    }
    assert(results.forall { case (_, matched) => !matched })
  }

  val PIZZA_PICASSO = "Picasso's Pizza"
  s"'${PIZZA_PICASSO}'" should "be close enough to some strategic words" in {
    val results =
      List().map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = PIZZA_PICASSO)) }
    results.foreach {
      case (anExpression, matched) =>
        if (!matched) info(s"'${PIZZA_PICASSO}' DID NOT match '${anExpression}', but it should have!")
    }
    assert(results.forall { case (_, matched) => matched })
  }

  it should "NOT be close to some silly words" in {
    val results =
      List("pizza pizzerias",
        "pizza").map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = PIZZA_PICASSO)) }
    results.foreach {
      case (anExpression, matched) =>
        if (matched) info(s"'${PIZZA_PICASSO}' matched '${anExpression}', but it shouldn't have!")
    }
    assert(results.forall { case (_, matched) => !matched })
  }

  val TUCCARO_TAZI = "Tuccaro's Taxi Service"
  s"'${TUCCARO_TAZI}'" should "be close enough to some strategic words" in {
    val results =
      List(TUCCARO_TAZI,
        "tuccaros taxi service").map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = TUCCARO_TAZI)) }
    results.foreach {
      case (anExpression, matched) =>
        if (!matched) info(s"'${TUCCARO_TAZI}' DID NOT match '${anExpression}', but it should have!")
    }
    assert(results.forall { case (_, matched) => matched })
  }

  it should "NOT be close to some silly words" in {
    val results =
      List("taxis").map { anExpression => (anExpression, stringMatcher.isCloseEnough(anExpression, toMatch = TUCCARO_TAZI)) }
    results.foreach {
      case (anExpression, matched) =>
        if (matched) info(s"'${TUCCARO_TAZI}' matched '${anExpression}', but it shouldn't have!")
    }
    assert(results.forall { case (_, matched) => !matched })
  }

}
