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
    // create a Spark session
    val spark = SparkSession.builder.appName("CSV Data Processor").getOrCreate()

    val resDf = getGroupByMonth(sourceDf)
    resDf.show()

    val expectedDf = Seq(
      (1, 2),
      (2,2)
    ).toDF("Month", "Number of flights")
    expectedDf.show()
    assert(assertData(resDf, expectedDf))
  }

//  test("100 most DataFrame Data Test") {
//    val sourceDf = Seq(
//      ("Jackie", "Ax", "19861126-29967"),
//      ("Vanessa", "Campball", "19881021-86591"),
//      ("Willetta", "Reneta", "19991125-38555")
//    ).toDF("name", "surname", "identification_number")
//
//    val resDf = getCSVDF(sourceDf)
//
//    val expectedDf = Seq(
//      ("Jackie", "Ax", "19861126-29967", "19861126", "1986", "11", "26"),
//      ("Vanessa", "Campball", "19881021-86591", "19881021", "1988", "10", "21"),
//      ("Willetta", "Reneta", "19991125-38555", "19991125", "1999", "11", "25")
//    ).toDF("name", "surname", "identification_number", "birth_date", "year", "month", "day")
//
//    assert(assertData(resDf, expectedDf))
//  }


//  test("Passengers flown together with argument Test"){}

// test("Passengers through all the countries Test"){}

// test("Passengers flown together Test")

}