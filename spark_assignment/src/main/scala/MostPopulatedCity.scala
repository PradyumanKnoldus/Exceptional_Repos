package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MostPopulatedCity extends App {
  val spark = SparkSession.builder()
    .appName("MostPopulatedCity")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/MostPopulatedCity.csv")
  df.show(false)

  val formattedDF = df.withColumn("population", regexp_replace(col("population"), " ", "").cast("integer"))
  formattedDF.show(false)

  val maxPopulationDF = formattedDF.groupBy("country").agg(max("population").as("maxPopulation"))
  maxPopulationDF.show(false)

  val semiResultDF = formattedDF.as("df1").join(maxPopulationDF.as("df2"), col("df1.country") === col("df2.country") && col("df1.population") === col("df2.maxPopulation"))
    .select(col("df1.name"), col("df2.country"), col("df2.maxPopulation"))
  semiResultDF.show(false)

  // RESULT
  semiResultDF.as("df1").join(df.as("df2"), col("df1.name") === col("df2.name"))
    .select(col("df1.name"), col("df1.country"), col("df2.population")).show(false)
}
