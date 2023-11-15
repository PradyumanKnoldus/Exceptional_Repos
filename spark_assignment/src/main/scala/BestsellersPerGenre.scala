package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BestsellersPerGenre extends App {
  val spark = SparkSession.builder()
    .appName("BestsellersPerGenre")
    .master("local[1]")
    .getOrCreate()

  val books = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/BestSeller.csv")

 val windowSpec = Window.partitionBy("genre").orderBy("id")

  val rankedDF = books.withColumn("Rank", rank.over(windowSpec))
  rankedDF.show(false)

  rankedDF.select("*").where(col("rank") === 1 || col("rank") === 2).show(false)
}
