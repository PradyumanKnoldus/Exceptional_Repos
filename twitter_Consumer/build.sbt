ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "twitter_Consumer",
    idePackagePrefix := Some("com.knoldus")
  )

libraryDependencies += "com.danielasfregola" %% "twitter4s" % "8.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.7"

