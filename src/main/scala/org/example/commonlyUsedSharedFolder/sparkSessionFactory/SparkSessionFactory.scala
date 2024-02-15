package org.example.commonlyUsedSharedFolder.sparkSessionFactory

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local")
      .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
  }
}
