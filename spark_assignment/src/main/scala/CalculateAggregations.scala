package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CalculateAggregations extends App {
  val spark = SparkSession.builder()
    .appName("CalculateAggregations")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    ("0", "Warsaw", "1 764 615"),
    ("1", "Villeneuve-Loubet", "15 020"),
    ("2", "Vranje", "83 524"),
    ("3", "Pittsburgh", "1 775 634")
  ).toDF("id", "name", "population")

  df.show(false)

  val formattedDF = df.withColumn("population", regexp_replace($"population"," ","").cast("integer"))
  formattedDF.show(false)
  formattedDF.printSchema()

  formattedDF.agg(max(col("population")).as("population")).show(false)

  // ----------------------------------------------------------------------------------------------------

  val maxPopulation = formattedDF.agg(max(col("population"))).head.getInt(0)

  formattedDF.select("name", "population").filter(col("population") === maxPopulation).show(false)
}
