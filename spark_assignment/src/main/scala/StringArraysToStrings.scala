package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StringArraysToStrings extends App {
  val spark = SparkSession.builder()
    .appName("StringArraysToStrings")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val words = Seq(Array("hello", "world")).toDF("words")
  words.show(false)

  words.withColumn("solution", concat_ws(" ", col("words"))).show(false)
}
