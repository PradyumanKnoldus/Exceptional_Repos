package com.knoldus
package functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("builtInFunctions")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
    (2, "Rose", 1, "2010", "20", "M", 4000),
    (3, "Williams", 1, "2010", "10", "M", 1000),
    (4, "Jones", 2, "2005", "10", "F", 2000),
    (5, "Brown", 2, "2010", "40", "", -1),
    (6, "Brown", 2, "2010", "50", "", -1)
  )
  val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
    "emp_dept_id", "gender", "salary")

  val empDF = emp.toDF(empColumns: _*)

  empDF.show(false)

  val dept = Seq(("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
  )

  val deptColumns = Seq("dept_name", "dept_id")
  val deptDF = dept.toDF(deptColumns: _*)

  deptDF.show(false)

  println("INNER JOIN")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").show(false)

  println("OUTER JOIN")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer").show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full").show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter").show(false)

  println("LEFT JOIN")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left").show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter").show(false)

  println("RIGHT JOIN")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right").show(false)
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter").show(false)

  println("LEFT SEMI")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi").show(false)

  println("LEFT ANTI")
  empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftanti").show(false)

  println("SELF JOIN")
  empDF.as("emp")
    .join(empDF.as("dept"),
      col("emp.superior_emp_id") === col("dept.emp_id"), "inner")
    .select(col("emp.emp_id"), col("emp.name"),
      col("dept.emp_id").as("superior_emp_id"),
      col("dept.name").as("superior_emp_name"))
    .show(false)

  println("CROSS JOIN")
  empDF.crossJoin(deptDF).show(false)

  empDF.joinWith(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").show(false)
}

