package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PartitioningFiles extends App {
  val spark = SparkSession.builder()
    .appName("DivideLargeFileIntoSmallFiles")
    .master("local[*]")
    .getOrCreate()

  // Read CSV File
  val csvPath = "/home/knoldus/IdeaProjects/SparkETL/src/main/resources/all_data.csv"
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "dropMalformed")
    .csv(csvPath)

  val cleanedDf = df.na.drop("any")

  // Extract Date from the Order Date
  val dfWithDate = cleanedDf.withColumn("Date", date_format(to_date(col("Order Date"), "MM/dd/yy HH:mm"), "dd/MM/yyyy"))

  // Repartition the DataFrame based on City
  val numPartitions = dfWithDate
    .select("Date")
    .distinct()
    .count().toInt

  val repartitionedDF = dfWithDate
    .repartition(numPartitions, col("Date"))
    .drop("Date")

  // Save each partition as a separate CSV file
  val outputDir = "/home/knoldus/IdeaProjects/SparkETL/src/main/partitionedFiles"
  repartitionedDF.write
    .option("header", "true")
    .mode("overwrite")
    .format("csv")
    .save(outputDir)

}
