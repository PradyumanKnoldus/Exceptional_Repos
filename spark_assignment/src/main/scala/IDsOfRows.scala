package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IDsOfRows extends App {
  val spark = SparkSession.builder()
    .appName("IDsOfRows")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    ("1","one,two,three","one"),
    ("2","four,one,five","six"),
    ("3","seven,nine,one,two","eight"),
    ("4","two,three,five","five"),
    ("5","six,five,one","seven")
  )

  val dataFrame = data.toDF("ID", "Words", "Word")
  dataFrame.show(false)

  val df = dataFrame.withColumn("words_array", split(col("Words"), ","))

  val explodedDF = df.withColumn("W", explode(col("words_array")))

  val collectedDF = explodedDF.groupBy("W").agg(collect_list("ID").as("IDs"))

  collectedDF.join(dataFrame, collectedDF("W") === dataFrame("Word"), "inner").drop("ID", "Words", "Word").show(false)
}
