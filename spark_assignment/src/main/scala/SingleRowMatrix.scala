package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SingleRowMatrix extends App {
  val spark = SparkSession.builder()
    .appName("SingleRowMatrix")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val df = Seq(
    (20090622,458),
    (20090624,31068),
    (20090626,151),
    (20090629,148),
    (20090914,453)
  ).toDF("udate","cc")

  df.show(false)

  val pivotDF = df.groupBy()
    .pivot("udate")
    .agg(first("cc"))
    .withColumn("udate", lit("cc"))


  pivotDF.show(false)

  pivotDF.select("udate", pivotDF.columns: _*).show(false)
}
