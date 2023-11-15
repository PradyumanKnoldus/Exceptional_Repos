package com.knoldus
package functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateTimeFunctions extends App {
  val spark = SparkSession.builder()
    .appName("DataFramesPractice")
    .master("local[1]")
    .getOrCreate()

  //-------------------------------------------------------------------------

  import spark.implicits._

  val data = Seq(
    (1, "Pradyuman"),
    (2, "Mohika"),
    (3, "Sant"),
    (4, "Manish"),
    (5, "Swantika"),
    (6, "Amit"),
    (7, "Rivyanshu"),
    (8, "Yash"),
    (9, "Ayush"),
    (10, "Piyush")
  )

  val df = data.toDF("ID", "Name")
  df.show(false)

  //-------------------------------------------------------------------------

  val newDf = df.withColumns(Map(
    "epochTimeStamp" -> unix_timestamp(),
    "currentTimeStamp" -> current_timestamp(),
    "currentDate" -> current_date()
  ))

  newDf.show(false)

  //-------------------------------------------------------------------------

  newDf.select($"ID", $"Name",
    from_unixtime($"epochTimeStamp", "dd-MM-yyyy").as("epochToDate"),
    unix_timestamp($"currentTimeStamp").as("timeStampToEpoch"),
    to_timestamp($"epochTimeStamp").as("epochToTimeStamp"),
    to_date($"currentTimeStamp").as("timeStampToDate(2)"))
    .show(false)

  //-------------------------------------------------------------------------

  val newData = Seq(("Pradyuman","1999-11-05"))
  val newestDf = newData.toDF("Name","DOB")
  newestDf.show(false)
  //-------------------------------------------------------------------------
//  val addData = Seq(("Male"))
//  val addDf = addData.toDF("Gender")
//  addDf.show(false)

  //-------------------------------------------------------------------------

  newestDf.select($"Name", $"DOB",date_add($"DOB", 3)).show(false)
  newestDf.select($"Name", $"DOB",add_months($"DOB", 3)).show(false)
  newestDf.select($"Name", $"DOB",localtimestamp()).show(false)
  newestDf.select($"Name", $"DOB",date_sub($"DOB", 3)).show(false)

  newestDf.select($"Name", $"DOB",datediff($"DOB", $"DOB")).show(false)

  newestDf.select($"Name", $"DOB",year($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",quarter($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",month($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",dayofweek($"DOB")).show(false)
  newestDf.select($"Name", $"DOB",dayofmonth($"DOB")).show(false)
  newestDf.select($"Name", $"DOB",dayofyear($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",hour($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",last_day($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",weekofyear($"DOB")).show(false)

  newestDf.select($"Name", $"DOB",weekofyear($"DOB")).show(false)

}


