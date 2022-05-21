lazy val root = (project in file("."))
  .settings(
    name := "Quantexa"
  )

lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala212)

scalaVersion := scala212
crossScalaVersions := supportedScalaVersions


cancelable := true

val sparkVersion = "3.0.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test withSources(),
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.7" % "test"

)
