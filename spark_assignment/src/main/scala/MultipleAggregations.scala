package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultipleAggregations extends App {
  val spark = SparkSession.builder()
    .appName("MultipleAggregations")
    .master("local[1]")
    .getOrCreate()

  val nums = spark.range(5).withColumn("group", col("id") % 2)
  nums.show(false)

  nums.groupBy("group").agg(max("id"), min("id")).show(false)
}
