package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FirstNonNullValue extends App {
  val spark = SparkSession.builder()
    .appName("FirstNonNullValue")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (None, 0),
    (None, 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

  df.show(false)

  df.groupBy("group").agg(first("id", ignoreNulls = true).as("First Non-Null")).show(false)
}
