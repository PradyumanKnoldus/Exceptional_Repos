package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IDsPerGroup extends App {
  val spark = SparkSession.builder()
    .appName("IDsPerGroup")
    .master("local[1]")
    .getOrCreate()

  val nums = spark.range(5).withColumn("group", col("id") % 2)
  nums.show(false)

  nums.groupBy("group").agg(collect_list("id").as("IDs")).show(false)
}
