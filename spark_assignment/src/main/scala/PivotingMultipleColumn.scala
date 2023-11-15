package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotingMultipleColumn extends App {
  val spark = SparkSession.builder()
    .appName("PivotingMultipleColumn")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (100, 1, 23, 10),
    (100, 2, 45, 11),
    (100, 3, 67, 12),
    (100, 4, 78, 13),
    (101, 1, 23, 10),
    (101, 2, 45, 13),
    (101, 3, 67, 14),
    (101, 4, 78, 15),
    (102, 1, 23, 10),
    (102, 2, 45, 11),
    (102, 3, 67, 16),
    (102, 4, 78, 18)).toDF("id", "day", "price", "units")

  df.show(false)

  //--------------------------------------------------------------------------------------

  val resultDF = df.groupBy("id")
    .pivot(col("day"))
    .agg(first("price").as("price"), first("units").as("units"))
    .orderBy("id")
  resultDF.show(false)

  //--------------------------------------------------------------------------------------

  val priceDF = df.groupBy("id")
    .pivot(col("day"))
    .agg(first("price"))
    .select(
      $"id",
      $"1".alias("price_1"),
      $"2".alias("price_2"),
      $"3".alias("price_3"),
      $"4".alias("price_4")
    )
  priceDF.show(false)

  val unitDF = df.groupBy("id")
    .pivot(col("day"))
    .agg(first("units"))
    .select(
      $"id",
      $"1".alias("units_1"),
      $"2".alias("units_2"),
      $"3".alias("units_3"),
      $"4".alias("units_4")
    )
  unitDF.show(false)

  priceDF.join(unitDF, "id").orderBy("id").show(false)

  //----------------------------------------------------------------------------------------

  val pricesDF = df.withColumn("tempCol", concat(lit("price_"), col("day")))
    .groupBy("id")
    .pivot(col("tempCol"))
    .agg(first("price"))
  pricesDF.show(false)

  val unitsDF = df.withColumn("tempCol", concat(lit("unit_"), col("day")))
    .groupBy("id")
    .pivot(col("tempCol"))
    .agg(first("units"))
  unitsDF.show(false)

  pricesDF.join(unitsDF, "id").orderBy("id").show(false)
}
