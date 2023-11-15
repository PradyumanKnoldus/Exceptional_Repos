package com.knoldus
package functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ArrayFunctions extends App {
  val spark = SparkSession.builder()
    .appName("builtInFunctions")
    .master("local[1]")
    .getOrCreate()

  val data = Seq((1,"Pradyuman", "23", "Prateek", "Book Management", "Akka-HTTP"), (2,"Mohika", "50", "Pragati", "User Management", "CRUD Operations"), (3,"Sant", "21", "Shubham", "User Management", "CRUD Operations"), (4,"Manish", "11", "Prakhar", "User Management", "Akka-HTTP"))
  val schema = Seq("ID", "Name","Age", "Mentors", "Project", "Learning")

  import spark.implicits._

  val df = data.toDF(schema: _*)
  df.show()

  val df2 = df.select(array(df.columns.map(col): _*))

  df2.printSchema()

  val dfGrouped = df.groupBy(col("Project"))
    .agg(collect_list("Learning").as("Mentorship Projects"))
  dfGrouped.show(false)

  val dfAgg = df.agg(collect_list("Learning").as("Latest Mentorship Projects"),
    avg("Age").as("Average Age"),
    sum("Age").as("Total Of Ages"),
    max("Age").as("Maximum Age"),
    min("Age").as("Minimum Age"))
  dfAgg.show(false)

  val array_contains_df = dfGrouped.withColumn("result", array_contains(col("Mentorship Projects"),"CRUD Operations"))
  array_contains_df.show(false)

  val array_contains_df2 = dfGrouped.select(array_contains(col("Mentorship Projects"), "CRUD Operations"))
  array_contains_df2.show(false)

  val array_append_df = dfGrouped.select(array_append(col("Mentorship Projects"),null))
  array_append_df.show(false)

  val array_compact_df = array_append_df.select(array_compact(col("array_append(Mentorship Projects, NULL)")))
  array_compact_df.show(false)

  val array_distinct_df = dfGrouped
    .select(array_append(col("Mentorship Projects"),"Spark Dataframe").as("Mentorship Projects"))
    .select(array_distinct(col("Mentorship Projects")))
  array_distinct_df.show(false)

  val array_element_at_df = dfGrouped.select(element_at(col("Mentorship Projects"),3))
  array_element_at_df.show(false)

  df.sort().orderBy("age").show(false)

  df.agg(mean(col("age")))

  df.agg(approx_count_distinct(col("age")).alias("divisionDistinct"))
  df.agg(approx_count_distinct(col("age"),0).alias("divisionDistinct"))
  df.agg(countDistinct(col("age")).alias("divisionDistinct"))
  df.select("age").dropDuplicates().count()
  df.select("age").distinct().count()

  df.selectExpr("ID * Age").show(false)
}
