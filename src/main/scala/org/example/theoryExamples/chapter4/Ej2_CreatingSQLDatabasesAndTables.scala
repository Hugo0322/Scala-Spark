package org.example.theoryExamples.chapter4

import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej2_CreatingSQLDatabasesAndTables {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Creating SQL Databases and Tables")

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val dataFrame = spark.read.schema(schema).csv(fileInjected)
    dataFrame.createOrReplaceTempView("unmanaged_us_delay_flights_tbl")

    // Creating the database
    //spark.sql("DROP DATABASE IF EXISTS learn_spark_db")
    //spark.sql("CREATE DATABASE learn_spark_db")
    // Pointing to the database we just created and will be using
    //spark.sql("USE learn_spark_db")

    // Creating a MANAGED table
    // spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    // Creating an UNMANAGED table
    spark.sql(s"""CREATE TABLE unmanaged_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (PATH '$fileInjected')""")

    //spark.sql("""CREATE EXTERNAL TABLE unmanaged_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING) PARTITIONED BY (geo STRING) OPTIONS (PATH '/scr/main/resources/tryOut/geo=s')""")
    //spark.sql("""REFRESH TABLE unmanaged_us_delay_flights_tbl""")

    spark.sql("""SELECT * FROM unmanaged_us_delay_flights_tbl""").show(10)


    // Creating views
    spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS SELECT date, delay, origin, destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'SFO'""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS SELECT date, delay, origin, destination FROM unmanaged_us_delay_flights_tbl WHERE origin = 'JFK'""")

    // After creating them you can issue queries against them
    spark.sql("""SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view""").show(10)
    spark.sql("""SELECT * FROM us_origin_airport_JFK_tmp_view""").show(10)

    // Dropping views
    //spark.sql("""DROP VIEW IF EXISTS global_temp.us_origin_airport_SFO_global_tmp_view""")
    //spark.sql("""DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view""")

    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

    // Accessing all the stored metadata
    spark.catalog.listDatabases()
    spark.catalog.listTables()
    spark.catalog.listColumns("unmanaged_us_delay_flights_tbl")

    // Caching SQL Tables
    spark.sql("""CACHE LAZY TABLE unmanaged_us_delay_flights_tbl""")
    spark.sql("""UNCACHE TABLE unmanaged_us_delay_flights_tbl""")

    // Reading Tables into DataFrames
    val usFlightsDF = spark.sql("SELECT * FROM unmanaged_us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("unmanaged_us_delay_flights_tbl")
    usFlightsDF2.show()
    usFlightsDF.show()



  }

}
