package QuantexaAir

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

//noinspection SpellCheckingInspection
object FlightsDataCalculation {

  /**
   * @param CSVfile - csv file location
   * @param spark - spark session
   * @return dataframe from csv file
   */
  def getCSVDF(CSVfile: String, spark: SparkSession): DataFrame = {
    val csvDF = spark.read.format("CSV")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(CSVfile)

    csvDF
  }

  /**
   * @param flightsdata  the flights data
   * @param atLeastNTimes minimum number of times passengers were together
   * @param from minimum date
   * @param to maximum date
   * @return dataframe that returns passengers that were in the flights together
   */
  def flownTogether(flightsdata: DataFrame, atLeastNTimes: Int, from: LocalDate, to: LocalDate): Dataset[Row] = {
    val flownTogetherDF = flightsdata
      .where(to_date(col("from"), "yyyy-MM-dd") >= from &&
        to_date(col("to"), "yyyy-MM-dd") <= to &&
        col("flightsTogether") >= atLeastNTimes
      )
    flownTogetherDF
  }


  /**
   * @param df flights dataframe
   * @return df that gives number of distinct flights per month considering we have only 2017's data
   */
  def getGroupByMonth(df: DataFrame): Dataset[Row] = {
    val flightsGroupeByMonthdDF: Dataset[Row] = df.groupBy(month(col("date")))
    .agg(countDistinct("flightId").as("Number of flights"))
    .withColumnRenamed("month(date)", "Month")
    .sort("Month")
    flightsGroupeByMonthdDF
  }    // flights calculation:

  /**
   * @param df flights data
   * @return df flights data with top 100 frequent flyers
   */
  def top100FrequentFlyers(df: DataFrame): Dataset[Row] = {
    val top100FrequentFlyersDF = df.groupBy("passengerId")
      .agg(countDistinct("flightId").as("Number of flights"))
      .orderBy(desc("Number of flights"))
      .limit(2) // set it to 2 for testing purpose
    top100FrequentFlyersDF
  }

  /**
   * @param df flights data
   * @return df with passengers that have been in the number of countries
   */
  def passengersLongestRun(df: DataFrame): Dataset[Row] ={
    //  Passengers with longest run
    // first we need to group by passenger to collect all his "from" and "to" countries
    val dataWithCountries = df.groupBy("passengerId")
      .agg(
        // concat is for concatenate two lists of strings from columns "from" and "to"
        concat(
          // collect list gathers all values from the given column into array
          collect_list(col("from")),
          collect_list(col("to"))
        ).name("countries")
      )

    val passengerLongestRuns = dataWithCountries.withColumn(
      "longest_run",
      size(array_remove(array_distinct(col("countries")), col("countries").getItem(0)))
    ).withColumn("uniqueCountries",
      array_remove(array_distinct(col("countries")), col("countries").getItem(0)))

    val passengerLongestRunsDF = passengerLongestRuns.withColumn("longestrunwithoutUK",
      size(array_remove(col("uniqueCountries"), "uk")))

    passengerLongestRunsDF
  }

  /**
   * @param df flights data
   * @return df that has passengers that were in the flights together
   */
  def getPassengersTogether(df: DataFrame): Dataset[Row] ={
    // Find the passengers who have been flown together. using flightsDF
    val passengersTogether = df.as("df1").join(df.as("df2"),
      col("df1.passengerId") < col("df2.passengerId") &&
        col("df1.flightId") === col("df2.flightId") &&
        col("df1.date") === col("df2.date"),
      "inner"
    ).groupBy(col("df1.passengerId"), col("df2.passengerId"))
      .agg(count("*").as("flightsTogether"), min(col("df1.date")).as("from"), max(col("df1.date")).as("to"))

    passengersTogether
  }


  def main(args: Array[String]): Unit = {

    // create a Spark session
    val spark = SparkSession.builder.appName("CSV Data Processor").getOrCreate()

    // flights file
    val flightsCSV = "src/main/scala/Data/flightData.csv"
    val flightsDF = getCSVDF(flightsCSV, spark)

    // passengers file
    val passengersCSV = "src/main/scala/Data/passengers.csv"
    val passengersDF = getCSVDF(passengersCSV, spark)

    // flights calculation: 
    // number of flights per month
    val flightsGroupeByMonthdDF = getGroupByMonth(flightsDF)
    flightsGroupeByMonthdDF.show()

    println("Saving flightsGroupeByMonthDF into csv file")
    flightsGroupeByMonthdDF.select("Month", "Number of flights")
      .coalesce(1).write.format("csv").mode("overwrite")
      .option("header", "true")
      .save("src/main/scala/output/flightsGroupeByMonthdDF")

    // join the df's using join function
    val combinedDF = flightsDF.join(passengersDF, usingColumn = "passengerId")
    //    combinedDF.show()

    // Find the names of the 100 most frequent flyers.
    val top100flyersDF = top100FrequentFlyers(combinedDF)

    val top100flyersDetailedDF = top100flyersDF.join(passengersDF, usingColumn = "passengerId")
    top100flyersDetailedDF.orderBy(desc("Number of flights")).show(101)

    println("saving top100flyersDetailedDF into csv file")
    top100flyersDetailedDF.select("passengerId", "Number of flights", "firstName","lastName")
      .coalesce(1).write.format("csv").mode("overwrite")
      .option("header", "true")
      .save("src/main/scala/output/top100flyersDetailedDF")

    // Passengers with longest run
    val passengerLongestRunsDF = passengersLongestRun(flightsDF)

    println("Save passengersLongestRun into csv file")
//    passengerLongestRunsDF.select("passengerId", "longest_run", "longestrunwithoutUK")
//      .coalesce(1).write.format("csv").mode("overwrite")
//      .save("src/main/scala/output/passengersLongestRun")

    // get passengers that have been in the flights together
    val passengersTogether = getPassengersTogether(flightsDF)

    // Passengers that have been in more than 3 flights together
    val passengersTogethergt3 = passengersTogether.where(col("flightsTogether") >= 3)

    println("saving passengersTogethergt3 into csv file")
    passengersTogethergt3.select(col("df1.passengerId").as("passengerId_1"),
      col("df2.passengerId").as("passengerId_2"), col("flightsTogether"))
      .coalesce(1).write.format("csv").mode("overwrite")
      .option("header", "true")
      .save("src/main/scala/output/passengersTogethergteq3")

    // convert strings to date to filter it
    val passengersTogethercvdate = passengersTogether
      .withColumn("from_date", to_date(col("from"), "yyyy-MM-dd"))
      .withColumn("to_date", to_date(col("to"), "yyyy-MM-dd"))

    val from = LocalDate.parse(
      "2017-01-01",
      DateTimeFormatter.ofPattern("yyyy-MM-dd")
    )
    val to = LocalDate.parse(
      "2017-01-31",
      DateTimeFormatter.ofPattern("yyyy-MM-dd")
    )
    // get the passengers who were in the same flights more than N times within given timeframe
    val passengersTogethergtn = flownTogether(passengersTogethercvdate, 4, from, to)
    println("parameterised passengers details")

    println("saving passengersTogethergtn into csv file")
    passengersTogethergtn.select(col("df1.passengerId").as("passengerId_1"),
      col("df2.passengerId").as("passengerId_2"), col("flightsTogether"),
      col("from"), col("to"))
      .coalesce(1).write.format("csv").mode("overwrite")
      .option("header", "true")
      .save("src/main/scala/output/passengersTogethergtn")

    // Stop spark session
    spark.stop()

  }
}
