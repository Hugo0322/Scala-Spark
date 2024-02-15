package org.example.chapter3

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory


object Ej2_FireCalls_CommonDataFrameOperations {

  def run (fsFireFile: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Fire Calls")

    val fireSchema = StructType(Array(
      StructField("CallNumber", StringType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)))

    // Read the file using the CSV DataFrameReader
    val fireDataFrame = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(fsFireFile)

    val parquetPath = "src/main/resources/DataFrameWriters/DataFrameWriter"
    fireDataFrame.write.mode("overwrite").format("parquet").save(parquetPath)

    val parquetTable = "StationArea"
    val parquetTablePath = "src/main/resources/DataFrameWriters/DataFrameTableWriter"
    fireDataFrame.write.mode("overwrite").format("parquet").option("path", parquetTablePath).saveAsTable(parquetTable)

    // Filtering the table data by where clausule and only selecting 3 columns
    val fewFireDataFrame = fireDataFrame
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(fireDataFrame("CallType") =!= "Medical Incident")

    fewFireDataFrame.show(5, false)

    // Counting how many different priority reasons for the calls counting only not null ones
    fireDataFrame.
      select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctPriorities")
      .show

    // Listing distinct call types in the DataSet
    fireDataFrame
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)

    // RENAMING, ADDING and DROPPING columns
    // Renaming
    val newFireDataFrame = fireDataFrame.withColumnRenamed("Delay", "ResposeDelayedMins")
    newFireDataFrame
      .select("ResposeDelayedMins")
      .where(newFireDataFrame("ResposeDelayedMins") > 5)
      .show(5, false)


    // Converting string to a Spark-supported timestamp, drop the string column and replace it by the timestamp one and assign the modified DF
    val fireTimeStampDataFrame = newFireDataFrame
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    // Selecting only those modified columns
    fireTimeStampDataFrame
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    // Now we can query this new columns
    /*
    fireTimeStampDataFrame
      .select(year(fireDataFrame("IncidentDate")))
      .distinct()
      .orderBy(year(fireDataFrame("IncidentDate")))
      .show()
     */

    // Showing most commont call types
    fireTimeStampDataFrame
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    // Other common DataFrame operations
    fireTimeStampDataFrame
      .select(F.sum("NumAlarms"), F.avg("ResposeDelayedMins"), F.min("ResposeDelayedMins"), F.max("ResposeDelayedMins")).show()



    // END-to-END DataFrame Example

      /*
            What were all the different types of fire calls in 2018?
            What months within the year 2018 saw the highest number of fire calls?
            Which neighborhood in San Francisco generated the most fire calls in 2018?
            Which neighborhoods had the worst response times to fire calls in 2018?
            Which week in the year in 2018 had the most fire calls?
            Is there a correlation between neighborhood, zip code, and number of fire calls?
            How can we use Parquet files or SQL tables to store this data and read it back?
       */

  }
}
