package org.example.chapter3

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, concat, expr}
import org.apache.spark.sql.types._
import org.example.sparkSessionFactory.SparkSessionFactory

object Ej1_DefiningSchema {

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

    import spark.implicits._

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


    // COLUMNS AND EXPRESSIONS //
    blogsDF.columns

    blogsDF.col("Id")

    // Both do basically the same
    blogsDF.select(expr("Hits * 2")).show(2)
    blogsDF.select(col("Hits") * 2).show(2)

    // Use an expression to compute big hitters for blogs. This adds a new column, Big Hitters, based on the conditional expression
    blogsDF.withColumn("Big Hitters", (expr("Hits > 1000"))).show()

    // Concatenate 3 columns, create a new column and show the newly created concatenated column
    blogsDF.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select("AuthorsId").show(4)

    // This statements return the same value, showing that expr is the same as col method call
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)

    // Shorting by column "Id" in descending order
    blogsDF.sort(col("Id").desc).show()
    // blogsDF.sort($"Id".desc).show() // Gives an error for whatever reason


    // ROWS //
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))

    // Access using index for individual items
    blogRow(1)

    // Row objects can be used to create DataFrames if you need the for quick interactivity and exploration
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = rows.toDF("Author", "State") // DUDA
    authorsDF.show()



  }
}
