package com.knoldus
package functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctions extends App {
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

  df.select(approx_count_distinct("salary").as("Approx Distinct Count")).show(false)

  df.select(avg("salary").as("Average Salary")).show(false)

  df.select(collect_list("salary").as("Collect List")).show(false)

  df.select(collect_set("salary").as("Collect Set")).show(false)

  df.select(countDistinct("salary").as("Distinct Count")).show(false)

  df.select(count("salary").as("Total Count")).show(false)

  df.select(first("salary").as("First Salary")).show(false)

  df.select(last("salary").as("Last Salary")).show(false)

  df.select(sum("salary").as("Salary Sum")).show(false)

  df.select(sum_distinct(col("salary")).as("Distinct Salary Sum")).show(false)

  df.groupBy("department").agg(sum("salary").as("Salary Sum"),
    avg("salary").as("Average Salary"),
    min("salary").as("Minimum Salary"),
    max("salary").as("Maximum Salary")).show(false)

  df.groupBy("department","Location").agg(sum("salary").as("Salary Sum"),
    avg("salary").as("Average Salary"),
    min("salary").as("Minimum Salary"),
    max("salary").as("Maximum Salary")).show(false)

  df.select(variance("salary").as("Variance")).show(false)
  df.select(var_samp("salary").as("Sample Variance")).show(false)

  df.select(var_pop("salary").as("Variance Population")).show(false)

  df.select(stddev("salary").as("Standard Deviation")).show(false)
  df.select(stddev_pop("salary").as("Standard Deviation Population")).show(false)
  df.select(stddev_samp("salary").as("Standard Deviation Sample")).show(false)

  df.select(skewness("salary").as("Skewness")).show(false)

  df.select(mean(col("salary")).as("Mean")).show(false)
  df.select(median(col("salary")).as("Median")).show(false)
  df.select(mode(col("salary")).as("Mode")).show(false)

  df.agg(mean(col("salary")).as("Mean of Salary")).show(false)
  df.agg(mean("salary").as("Mean of Salary")).show(false)

//  df.select(min_by(col("salary"),col("location"))).show(false)
//  df.select(max_by(col("salary"),col("location"))).show(false)
}
