package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AddDaysToDate extends App {
  val spark = SparkSession.builder()
    .appName("AddDaysToDate")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val dataDF = Seq(
    (0, "2016-01-1"),
    (1, "2016-02-2"),
    (2, "2016-03-22"),
    (3, "2016-04-25"),
    (4, "2016-05-21"),
    (5, "2016-06-1"),
    (6, "2016-03-21")
  ).toDF("number_of_days", "date")

  dataDF.show(false)

  val futureDF = dataDF.withColumn("Future", date_add(col("date"), col("number_of_days")))
  futureDF.show(false)
}
