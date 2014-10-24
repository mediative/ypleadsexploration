package util

import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import org.joda.time.DateTime
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import util.Util._

class BaseTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  val sleepTimeInSecs = 1
  s"A call that sleeps for ${sleepTimeInSecs} second(s)" should s"take about ${sleepTimeInSecs}*10^9 ns., at better than 0.1% accuracy" in {
    val (t, _) = time({ Thread.sleep(sleepTimeInSecs * 1000) })
    val sleepTimeInNanoSecs = sleepTimeInSecs * 1E9
    val accuracy = ((math.abs(t - sleepTimeInNanoSecs) * 100) / sleepTimeInNanoSecs)
    withClue(s"Accuracy = ${accuracy} %") { assert(accuracy < 0.1) }
  }

  val pizzaWords = Set("PIZZA")
  val dominosPizza = "Dominos Pizza"
  s"Filtering occurrences of ${pizzaWords.mkString(start = "{", sep = ",", end = "}")} from '${dominosPizza}'" should "yield 'Dominos' when match is case-insensitive" in {
    assert(String.filterOccurrencesFrom(wordsToFilter = pizzaWords, aString = dominosPizza, caseSensitive = false) == "Dominos")
  }

  it should "yield 'Dominos Pizza' when match is case-sensitive" in {
    assert(String.filterOccurrencesFrom(wordsToFilter = pizzaWords, aString = dominosPizza, caseSensitive = true) == dominosPizza)
  }

  "Days mentioned today" should "be 1" in {
    val now = DateTime.now
    assert(Date.getAllDays(fromDate = now, toDate = now).length == 1)
  }

  it should "be 1, even if we are comparing with *beginning* of day" in {
    val now = DateTime.now
    val todaysFirstSecond = now.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    assert(Date.getAllDays(fromDate = now, toDate = todaysFirstSecond).length == 1)
  }

  it should "be 1, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val todaysLastSecond = now.withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    assert(Date.getAllDays(fromDate = now, toDate = todaysLastSecond).length == 1)
  }

  "Days mentioned between yesterday and today" should "be 2 when time is the same" in {
    val now = DateTime.now
    val yesterdaySameTime = now.minusDays(1)
    withClue(s"From = ${now}, To = ${yesterdaySameTime}") { assert(Date.getAllDays(fromDate = yesterdaySameTime, toDate = now).length == 2) }
  }

  it should "be 2, even if we are looking at *beginning* of day" in {
    val now = DateTime.now
    val yesterdayFirstSecond = now.minusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    withClue(s"From = ${now}, To = ${yesterdayFirstSecond}") { assert(Date.getAllDays(fromDate = yesterdayFirstSecond, toDate = now).length == 2) }
  }

  it should "be 2, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val yesterdayLastSecond = now.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    withClue(s"From = ${now}, To = ${yesterdayLastSecond}") { assert(Date.getAllDays(fromDate = yesterdayLastSecond, toDate = now).length == 2) }
  }

  "Days mentioned between today and yesterday (ie, going BACKWARDS)" should "be 0 when time is the same" in {
    val now = DateTime.now
    val yesterdaySameTime = now.minusDays(1)
    withClue(s"From = ${now}, To = ${yesterdaySameTime}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdaySameTime).length == 0) }
  }

  it should "be 0, even if we are looking at *beginning* of day" in {
    val now = DateTime.now
    val yesterdayFirstSecond = now.minusDays(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(1)
    withClue(s"From = ${now}, To = ${yesterdayFirstSecond}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdayFirstSecond).length == 0) }
  }

  it should "be 0, even if we are looking at *end* of day" in {
    val now = DateTime.now
    val yesterdayLastSecond = now.minusDays(1).withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59)
    withClue(s"From = ${now}, To = ${yesterdayLastSecond}") { assert(Date.getAllDays(fromDate = now, toDate = yesterdayLastSecond).length == 0) }
  }

  // NB: my intention here is not to unit-test the LevenshteinMetric (since I am using somebody else's code)
  // but rather to check if it still complies with my needs
  "Levenshtein distance on two identical strings" should "be 0" in {
    List("luis", "whatever", "satisfaction", "!2747!@#*") foreach { s =>
      LevenshteinMetric.compare(s, s) match {
        case None => withClue(s"Comparison of ==> ${s} <== with itself returned None") { assert(false) }
        case Some(dist) => assert(dist == 0)
      }
    }
  }

  "Transformation of int to string" should "work with months and days" in {
    (1 to 31).foreach { dd =>
      assert(getIntAsString(dd, 2) == {
        if (dd < 10) ("0" + dd) else dd.toString
      })
    }
  }

}

