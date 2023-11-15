package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PercentRank extends App {
  val spark = SparkSession.builder()
    .appName("PercentRank")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/PercentRank.csv")
  df.show(false)

  val windowSpec = Window.orderBy(col("Salary").desc)

  val rankedDF = df.withColumn("Percentage", percent_rank().over(windowSpec))

  rankedDF.withColumn("Percentage", when(col("Percentage") <= 0.30, "High")
    .when(col("Percentage") <= 0.40, "Average")
    .otherwise("Low")).show(false)

}
