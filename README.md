# flightsDataCalcaulation

This is the repo to calculate flights data.

There are mainly 4 tasks to achieve.

1. Find the total number of flights for each month.
2. Find the names of the 100 most frequent flyers.
3. Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.
4. Find the passengers who have been on more than 3 flights together.

### Prerequisite 

Install the following
1. Scala - language (version : 2.12)
   1. sbt build 
2. Spark

Recommended IDE : IntelliJ

### Run 

    sbt package
    
    spark-submit   --class "QuantexaAir.FlightsDataCalculation"  --master local[4] "target\scala-2.12\quantexa_2.12-0.1.0-SNAPSHOT.jar"


here QuntexaAir is scala main directory and flightsDataCalculation is scala file that contains the logic
and jar is what sbt package command creates.

#### Testing

Functions have been tested using assert dataframe.
please refer to src/test package for more information.

Data has been saved into src/main/output folder.
Each folder contains specific task file into csv.

1. flightsGroupeByMonthDF for 1st task
2. top100flyersDetailedDF for 2nd task
3. passengersTogethergteq3 for 3rd task
4. passengersToethergtn for 4th task