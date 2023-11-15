ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "starting-with-spark",
    idePackagePrefix := Some("com.knoldus")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
//libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
