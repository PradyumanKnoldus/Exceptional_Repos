package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OpenAndCloseHours extends App {
  val spark = SparkSession.builder()
    .appName("OpenAndCloseHours")
    .master("local[1]")
    .getOrCreate()

  val jsonData =
    """
  {
    "business_id": "abc",
    "full_address": "random_address",
    "hours": {
      "Monday": {
        "close": "02:00",
        "open": "11:00"
      },
      "Tuesday": {
        "close": "02:00",
        "open": "11:00"
      },
      "Friday": {
        "close": "02:00",
        "open": "11:00"
      },
      "Wednesday": {
        "close": "02:00",
        "open": "11:00"
      },
      "Thursday": {
        "close": "02:00",
        "open": "11:00"
      },
      "Sunday": {
        "close": "00:00",
        "open": "11:00"
      },
      "Saturday": {
        "close": "02:00",
        "open": "11:00"
      }
    }
  }
"""

  import spark.implicits._

  val dataFrame = spark.read.json(Seq(jsonData).toDS())
  dataFrame.show(false)

  dataFrame.printSchema

  // Explode the hours struct into separate rows
  val explodedDF = dataFrame.select(
    col("business_id"),
    col("full_address"),
    explode(map(
      lit("Monday"), col("hours.Monday"),
      lit("Tuesday"), col("hours.Tuesday"),
      lit("Wednesday"), col("hours.Wednesday"),
      lit("Thursday"), col("hours.Thursday"),
      lit("Friday"), col("hours.Friday"),
      lit("Saturday"), col("hours.Saturday"),
      lit("Sunday"), col("hours.Sunday")
    )).as(Seq("day", "hours_struct"))
  )
    .select(
      col("business_id"),
      col("full_address"),
      col("day"),
      col("hours_struct.open").as("open"),
      col("hours_struct.close").as("close")
    )

  explodedDF.show(false)
}