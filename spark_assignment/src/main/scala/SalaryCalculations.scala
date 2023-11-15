package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalaryCalculations extends App{
  val spark = SparkSession.builder()
    .appName("SalaryCalculations")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.option("header", value = true).option("inferSchema", value = true)
    .csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/SalaryCalculations")

  df.show(false)

  df.rollup("department")
    .agg(sum("salary").as("sum"), avg("salary").as("avg"))
    .sort(col("sum").desc)
    .show(false)
}
