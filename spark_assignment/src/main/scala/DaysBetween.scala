package com.knoldus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DaysBetween extends App {
  val spark = SparkSession.builder()
    .appName("DaysBetween")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val datesDF = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")
  datesDF.show(false)

  val formattedDF = datesDF.withColumn("date_format", date_format(to_date(col("date_string"),"dd/MM/yyyy"),"yyyy-MM-dd"))
  formattedDF.show(false)

  formattedDF.withColumn("Diff", datediff(current_date(), col("date_format"))).show(false)

}
