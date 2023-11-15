package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MaxValuesPerGroup extends App {
  val spark = SparkSession.builder()
    .appName("MaxValuesPerGroup")
    .master("local[1]")
    .getOrCreate()

  val nums = spark.range(5).withColumn("group",  col("id") % 2)
  nums.show(false)

  nums.groupBy("group").max("id").show(false)

}
