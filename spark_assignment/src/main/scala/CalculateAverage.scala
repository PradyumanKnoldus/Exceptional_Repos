package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CalculateAverage extends App {
  val spark = SparkSession.builder()
    .appName("CalculateAverage")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (0, "A", 223, "201603", "PORT"),
    (0, "A", 22, "201602", "PORT"),
    (0, "A", 422, "201601", "DOCK"),
    (1, "B", 3213, "201602", "DOCK"),
    (1, "B", 3213, "201601", "PORT"),
    (2, "C", 2321, "201601", "DOCK")
  ).toDF("id", "type", "cost", "date", "ship")

  df.show(false)

  df.groupBy("id", "type")
    .pivot("date")
    .avg("cost")
    .orderBy("id")
    .show(false)

  df.groupBy("id", "type")
    .pivot("date")
    .agg(collect_list("ship"))
    .orderBy("id")
    .show(false)
}
