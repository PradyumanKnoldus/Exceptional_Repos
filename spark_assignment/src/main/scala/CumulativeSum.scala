package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CumulativeSum extends App {
  val spark = SparkSession.builder()
    .appName("CumulativeSum")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/CumulativeSum.csv")
  df.show(false)

  val windowSpec = Window.partitionBy("department")
    .orderBy("department")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  df.withColumn("cumulative_total", sum("items_sold")
    .over(windowSpec)).show(false)
}



