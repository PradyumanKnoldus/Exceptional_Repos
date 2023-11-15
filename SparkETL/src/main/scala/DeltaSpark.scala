package com.knoldus

import org.apache.spark.sql.SparkSession


object DeltaSpark extends App {

  import org.apache.spark.sql.delta.sources.DeltaDataSource
  // Initialize a Spark session
  val spark = SparkSession.builder()
    .appName("Delta Sharing Example")
    .master("local[*]")
    .config("spark.databricks.delta.preview.enabled", "true")
    .getOrCreate()

  // Set the path to the configuration file you shared
  val deltaSharingConfigPath = "/home/knoldus/IdeaProjects/SparkETL/src/main/resources/config.share"

  // Load the Delta Sharing configuration
  spark.conf.set("spark.databricks.delta.sharing.client.server.endpoint", deltaSharingConfigPath)

  // Define the identifier for the shared data you want to access
  val sharingTableIdentifier = "checkout_data_products.data_products.popular_products_data"

  // Read data from the shared table
  val sharedData = spark.read.format("delta").load(sharingTableIdentifier)

  // You can now use sharedData for further processing
  sharedData.show()
}
