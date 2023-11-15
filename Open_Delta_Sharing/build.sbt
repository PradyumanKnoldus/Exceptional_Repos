ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "Open_Delta_Sharing",
    idePackagePrefix := Some("com.knoldus")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "io.delta" %% "delta-sharing-client" % "1.0.1"
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
