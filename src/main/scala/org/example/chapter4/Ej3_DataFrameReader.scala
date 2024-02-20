package org.example.chapter4

import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory

object Ej3_DataFrameReader {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Data frame readers")

    val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"


    val dataFrame = spark.read.format("parquet").load(fileInjected + "2010-sumary.parquet")
    dataFrame.show()
    // Use Parquet; you can omit format("parquet") as it is the default
    val dataFrame2 = spark.read.load(fileInjected + "2010-sumary.parquet")
    dataFrame2.show()

    val dataFrame3 = spark.read.format("csv")
      .option("iferSchema", true)
      .option("header", true)
      .option("mode", "PERMISSIVE")
      .load(fileInjected + "csv/*")

    dataFrame3.show()

    val dataFrame4 = spark.read.format("json").load(fileInjected + "json/*")
    // dataFrame4.show() // Gives an error

    // Creating a temporary view from the parquet file
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet OPTIONS (PATH 'src/main/resources/in/sumaryData/2010-sumary.parquet')""")
    // Accessing and showing data from the temp view
    spark.sql("""SELECT * FROM us_delay_flights_tbl WHERE count = 54""").show()
    // Writing DataFrames to Parquet Files
    dataFrame2.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("src/main/resources/out/chapter4_Ej3/parquet")
    // Writing DataFrames to Spark SQL Tables
    //dataFrame.write
    //  .mode("overwrite")
    //  .saveAsTable("us_delay_flights_tbl")

    // Reading a JSON file into a DataFrame
    val dataFrameJSON = spark.read.format("json").load("src/main/resources/in/sumaryData/json/*")
    // Creating SQL temp table from a JSON
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING json OPTIONS (PATH 'src/main/resources/in/sumaryData/json/*')""")
    // spark.sql("""SELECT * FROM us_delay_flights_tbl""").show() //Throws an error
    // Writing DataFrames to JSON files
    //dataFrameJSON.write.format("json")
    //  .mode("overwrite")
    //  .option("compression", true)
    //  .save("src/main/resources/out/chapter4_Ej3/json")
    // Throws an error

    // Reading a CSV file into a DataFrame
    val dataFrameCSV = spark.read.schema(schema).option("header", "true").option("mode", "FAILFAST").option("nullValue", "").csv(fileInjected + "csv/*")
    dataFrameCSV.show()

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING csv OPTIONS (PATH 'src/main/resources/in/sumaryData/csv/*', header "true", inferSchema "true", mode "FAILFAST")""")
    spark.sql("""SELECT * FROM us_delay_flights_tbl WHERE count = 1""").show()

    dataFrameCSV.write.mode("overwrite").csv("src/main/resources/out/chapter4_Ej3/csv")

    // I didnt copied AVRO, ORC


  }

}
