import QuantexaAir.FlightsDataCalculation.{flownTogether, getGroupByMonth, top100FrequentFlyers,
  passengersLongestRun, getPassengersTogether}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


class DataParserTest extends AnyFunSuite with SparkSessionTestWrapper with DataFrameTestUtils {

  import spark.implicits._

  test("Compute total flights by Month Test") {

    val sourceDf = Seq(
        ("48",	0,	"cg",	"ir",	"2019-01-01"),
        ("94",	1,	"cg",	"ir",	"2019-01-01"),
        ("82",	0,	"cg",	"ir",	"2019-02-01"),
        ("21",	3,	"cg",	"ir",	"2019-02-01")
    ).toDF("passengerId",	"flightId",	"from",	"to",	"date")

    sourceDf.show()

    val resDf = getGroupByMonth(sourceDf)
    resDf.show()

    val expectedDf = Seq(
      (1, 2),
      (2,2)
    ).toDF("Month", "Number of flights")
    expectedDf.show()
    assert(assertData(resDf, expectedDf))
  }

  test("100 most DataFrame Data Test") {
    // testing purpose limit would be for 2 out of 8 records
    val sourceDf = Seq(
      ("48",	0,	"cg",	"ir",	"2019-01-01"),
      ("94",	1,	"cg",	"ir",	"2019-01-01"),
      ("82",	0,	"cg",	"ir",	"2019-02-01"),
      ("21",	3,	"cg",	"ir",	"2019-02-01"),
      ("48",	3,	"cg",	"ir",	"2019-01-01"),
      ("48",	1,	"cg",	"ir",	"2019-01-01"),
      ("82",	1,	"cg",	"ir",	"2019-02-01"),
      ("22",	3,	"cg",	"ir",	"2019-02-01")
    ).toDF("passengerId",	"flightId",	"from",	"to",	"date")

    sourceDf.show()

    val resDf = top100FrequentFlyers(sourceDf)
    resDf.show()
    val expectedDf = Seq(
      ("48", 3),
      ("82", 2)
    ).toDF("passengerId", "Number of flights")
    expectedDf.show()
    assert(assertData(resDf, expectedDf))
  }


  test("Passengers flown together with argument Test"){
    val sourceDf = Seq(
      ("218", "272",4, "2017-01-01","2017-01-31","2017-01-01", "2017-01-31"),
      ("123", "272",1, "2017-01-01","2017-02-31","2017-01-01", "2017-02-31"),
      ("216", "270",2, "2017-01-01","2017-03-31","2017-01-01", "2017-03-31"),
      ("211", "262",9, "2017-01-01","2017-04-31","2017-01-01", "2017-04-31"),
      ("212", "252",4, "2017-01-01","2017-01-31","2017-01-01", "2017-01-31")
    ).toDF("passengerId","passengerId","flightsTogether","from","to"," from_date","to_date")

    sourceDf.show()
    val from = LocalDate.parse(
      "2017-01-01",
      DateTimeFormatter.ofPattern("yyyy-MM-dd")
    )
    val to = LocalDate.parse(
      "2017-01-31",
      DateTimeFormatter.ofPattern("yyyy-MM-dd")
    )
    val resDf = flownTogether(sourceDf, 2, from, to)
    resDf.show()
    resDf.printSchema()

    val expectedDf = Seq(
      ("218", "272",4, "2017-01-01","2017-01-31","2017-01-01", "2017-01-31"),
      ("212", "252",4, "2017-01-01","2017-01-31","2017-01-01", "2017-01-31")
    ).toDF("passengerId","passengerId","flightsTogether","from","to"," from_date","to_date")

    expectedDf.show()
    expectedDf.printSchema()
    assert(assertData(resDf, expectedDf))
  }
//
 test("Passengers through all the countries Test"){
   val sourceDf = Seq(
     ("48",	0,	"cg",	"ir",	"2019-01-01"),
     ("94",	1,	"cg",	"ir",	"2019-01-02"),
     ("48",	4,	"ir",	"uk",	"2019-02-03"),
     ("21",	3,	"cg",	"ir",	"2019-02-04"),
     ("48",	5,	"uk",	"cr",	"2019-02-08"),
     ("94",	6,	"ir",	"ck",	"2019-01-06")
   ).toDF("passengerId",	"flightId",	"from",	"to",	"date")

   sourceDf.show()

   val resDf = passengersLongestRun(sourceDf)
   resDf.show(false)
   resDf.printSchema()

   val expectedDF = Seq(
     ("48", Array("cg", "ir", "uk", "ir", "uk", "cr"), 3, Array("ir", "uk", "cr"), 2),
     ("21", Array("cg", "ir"), 1, Array("ir"), 1),
     ("94", Array("cg", "ir", "ir", "ck"), 2, Array("ir", "ck"), 2)
   ).toDF("passengerId", "countries", "longest_run", "uniqueCountries", "longestrunwithoutUK")
   expectedDF.show(false)
   expectedDF.printSchema()
   assert(assertData(resDf, expectedDF))
 }
//
 test("Passengers flown together Test"){}
  val sourceDf = Seq(
    ("48",	0,	"cg",	"ir",	"2019-01-01"),
    ("94",	1,	"cg",	"im",	"2019-02-01"),
    ("82",	0,	"cg",	"ir",	"2019-01-01"),
    ("21",	3,	"cg",	"kr",	"2019-03-01")
  ).toDF("passengerId",	"flightId",	"from",	"to",	"date")

  println("5 th test")
  sourceDf.show()

  val resDf = getPassengersTogether(sourceDf)
  resDf.show()

  val expectedDF = Seq(
    ("48", "82", 1, "2019-01-01", "2019-01-01")
  ).toDF("passengersId", "passengersId", "flightsTogether", "from", "to")

  assert(assertData(resDf, expectedDF))
}
