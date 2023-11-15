package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object SalaryGap extends App {
  val spark = SparkSession.builder()
    .appName("SalaryGap")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/SalaryGap.csv")
  df.show(false)

  val windowSpec = Window.partitionBy("department").orderBy("department")

  df.withColumn("diff", max("salary").over(windowSpec) - col("salary")).show(false)
}
