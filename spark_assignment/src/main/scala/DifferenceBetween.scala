package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DifferenceBetween extends App {
  val spark = SparkSession.builder()
    .appName("DifferenceBetween")
    .master("local[1]")
    .getOrCreate()


  val df = spark.read.option("header", value = true).option("inferSchema", value = true)
    .csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/DifferenceBetween.csv")

  df.show(false)

  val windowSpec = Window.partitionBy("department").orderBy("department")

  val previousValue = lag(col("running_total"), 1, 0).over(windowSpec)

  df.withColumn("diff", (col("running_total") - previousValue)).show(false)
}
