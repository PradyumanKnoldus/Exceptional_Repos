package com.knoldus

import org.apache.spark.sql.SparkSession

object MergePartitionedFiles extends App {
  val spark = SparkSession.builder()
    .appName("MergeSmallFilesIntoSingleFile")
    .master("local[*]")
    .getOrCreate()

  // Read CSV File
  val csvPath = "/home/knoldus/IdeaProjects/SparkETL/src/main/partitionedFiles"
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csvPath)

  df.coalesce(1).write
    .option("header", "true")
    .format("csv")
    .mode("overwrite")
    .save("/home/knoldus/IdeaProjects/SparkETL/src/main/mergedFile")

}
