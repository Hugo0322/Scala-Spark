package org.example.fuf.sparkSessionFactory

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def createSparkSession(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local")
      //.enableHiveSupport()
      //.config("spark.sql.catalogImplementation","hive")
      .config("spark.executor.memory", "2g")
      .config("spark.executor.instances", "4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

      spark
  }
}
