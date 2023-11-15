package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExamAssessmentReport extends App {
  val spark = SparkSession.builder()
    .appName("ExamAssessmentReport")
    .master("local[1]")
    .getOrCreate()

  val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/home/knoldus/IdeaProjects/spark_assignment/src/main/resources/ExamAssessmentReport.csv")
  df.show(false)

  df.withColumn("tempCol", concat(lit("Qid_"), col("Qid")))
    .groupBy("ParticipantID", "Assessment", "GeoTag")
    .pivot("tempCol")
    .agg(first("AnswerText").as("Qid"))
    .show(false)
}
