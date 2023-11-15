ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "SparkETL",
    idePackagePrefix := Some("com.knoldus")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "io.delta" %% "delta-sharing-client" % "1.0.2"
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"

libraryDependencies += "io.delta" %% "delta-spark" % "3.0.0"
