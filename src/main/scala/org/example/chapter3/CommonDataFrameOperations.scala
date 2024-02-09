package org.example.chapter3

import org.apache.spark.sql.types._
import org.example.sparkSessionFactory.SparkSessionFactory

object CommonDataFrameOperations {

  def run (fsFireFile: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Fire Calls")

    val fireSchema = StructType(Array(
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
    fireDataFrame.write.format("parquet").save(parquetPath)

    val parquetTable = "StationArea"
    val parquetTablePath = "src/main/resources/DataFrameWriters/DataFrameTableWriter"
    fireDataFrame.write.format("parquet").option("path", parquetTablePath).saveAsTable(parquetTable)

  }

}
