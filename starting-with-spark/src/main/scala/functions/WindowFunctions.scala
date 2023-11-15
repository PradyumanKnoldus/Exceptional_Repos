package com.knoldus
package functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctions extends App {
  val spark = SparkSession.builder()
    .appName("builtInFunctions")
    .master("local[1]")
    .getOrCreate()

  val data: Seq[(String, String, Int, String)] = Seq(
    ("James", "Sales", 3000, "Delhi"),
    ("Michael", "Sales", 4600, "Gurugram"),
    ("Robert", "Sales", 4100, "Noida"),
    ("Maria", "Finance", 3000, "Delhi"),
    ("James", "Sales", 3000, "Gurugram"),
    ("Scott", "Finance", 3300, "Noida"),
    ("Jen", "Finance", 3900, "Gurugram"),
    ("Jeff", "Marketing", 3000, "Delhi"),
    ("Kumar", "Marketing", 2000, "Noida"),
    ("Saif", "Sales", 4100, "Delhi")
  )

  import spark.implicits._

  val df = data.toDF("employee_name", "department", "salary", "location")
  df.show()

  val windowSpec = Window.partitionBy("department").orderBy("salary")

  df.withColumn("row_number", row_number().over(windowSpec)).show(false)

  df.withColumn("cume_dist", cume_dist().over(windowSpec)).show(false)

  df.withColumn("rank", rank().over(windowSpec)).show(false)

  df.withColumn("dense_rank", dense_rank().over(windowSpec)).show(false)

  df.withColumn("nth value", nth_value(col("department"), 2).over(windowSpec))
    .show(false)

  df.withColumn("ntile", ntile(2).over(windowSpec)).show(false)


  val windowSpecLeadLag = Window.partitionBy("location", "salary").orderBy("salary")

  df.withColumn("lag", lag("salary", 2).over(windowSpecLeadLag)).show()

  df.withColumn("lead", lead("salary", 1).over(windowSpecLeadLag)).show()



  val windowSpecStats = Window.partitionBy("location", "department")

  val df_stats = df
    .withColumn("row_number", row_number.over(windowSpecStats.orderBy("salary")))
    .withColumn("Average", avg("salary").over(windowSpecStats))
    .withColumn("Sum", sum("salary").over(windowSpecStats))
    .withColumn("Min", min("salary").over(windowSpecStats))
    .withColumn("Max", max("salary").over(windowSpecStats))

  df_stats.show(false)

  df_stats.select("*").where(col("row_number") === 1).show(false)
}


