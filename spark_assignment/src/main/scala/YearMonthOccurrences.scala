package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object YearMonthOccurrences extends App {
  val spark = SparkSession.builder()
    .appName("YearMonthOccurrences")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val dataDF = Seq(
    (202001,500),
    (202001,600),
    (201912,100),
    (201910,200),
    (201910,100),
    (201910,100),
    (201910,100)
  ).toDF("YEAR_MONTH","AMOUNT")
  dataDF.show(false)

  val formattedDF = dataDF.withColumn("YEAR_MONTH", date_format(to_date(col("YEAR_MONTH"), "yyyyMM"), "yyyy-MM"))
  formattedDF.show(false)

  val startDate = to_date(lit(formattedDF.select("YEAR_MONTH").head().getString(0)))
  println(startDate)

  val intermediateDF = spark.range(0, 24)
    .select(add_months(startDate, -$"id").alias("Date"))
    .withColumn("Date", date_format($"date", "yyyy-MM"))
  intermediateDF.show(false)

  val finalDF = intermediateDF.join(formattedDF, formattedDF("YEAR_MONTH") === intermediateDF("Date"), "Left")
  finalDF.show(24,truncate = false)

  val droppedDF = finalDF.drop("YEAR_MONTH")
  droppedDF.show(24,truncate = false)

  val groupedDF = droppedDF.groupBy("Date").sum("AMOUNT").as("amount")
  groupedDF.show(24,truncate = false)

  val nullRemovedDF = groupedDF.na.fill(0)
  nullRemovedDF.show(24, truncate = false)

  val resultDF = nullRemovedDF.withColumn("Date", date_format($"Date", "yyyyMM"))
  resultDF.show(24, truncate = false)
}
