ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "udemy-akka-essentials"
  )

val akkaVersion = "2.8.0"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,                // Akka Actors
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,              // Testing Framework for Actors
  "org.scalatest" %% "scalatest" % "3.2.15",                        // Normal Testing Framework
  "org.scalafx" %% "scalafx" % "16.0.0-R25"                         // For GUI
)

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
