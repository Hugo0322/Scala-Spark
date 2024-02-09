package org.example.chapter3

import org.apache.spark.sql.types._
import org.example.sparkSessionFactory.SparkSessionFactory

object DefiningSchema {

  // Theres 2 ways to define a schema:
  // Programmatically for a DataFrame
  /*
   * val schema = StructType(Array(StructField("author", StringType, false),
   * StructField("title", StringType, false),
   * StructField("pages", IntegerType, false)))
  */
  // Using DDL (is much simpler)
  // val schema = "author STRING, title STRING, pages INT"

  def run (fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Schemas and injecting data")

    spark.sparkContext.setLogLevel("ERROR")

    // Defining the schema programmatically
    val schema = StructType(Array(
      StructField("Id", StringType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("URL", StringType, false),
      StructField("Publisher", StringType, false),
      StructField("Hits", StringType, false),
      StructField("Campaigns", StringType, false)
    ))

    // Creating a DataFrame by reading from the JSON file with a predefined schema which we just defined
    val blogsDF = spark.read.schema(schema).json(fileInjected)

    // Show all the data that has been read from the json file
    blogsDF.show(false)

    // Printing the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}
