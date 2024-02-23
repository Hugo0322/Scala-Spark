package org.example.theoryExamples.chapter5

import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej3_MySQL {

  def run(): Unit = {

    // Each of the [] should be changed for the actual names

   val spark = SparkSessionFactory.createSparkSession("MySQL")

   // Loading data from a JDBC source using load
   val jdbcDF = spark.read.format("jdbc")
     .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABSE]")
     .option("driver", "com.mysql.jdbc.Driver")
     .option("dbtable", "[TABLENAME]")
     .option("user", "[USERNAME]")
     .option("password", "[PASSWORD]")
     .load()

   // Saving data to a JDBC source using save
   jdbcDF.write.format("jdbc")
     .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABSE]")
     .option("driver", "com.mysql.jdbc.Driver")
     .option("dbtable", "[TABLENAME]")
     .option("user", "[USERNAME]")
     .option("password", "[PASSWORD]")
     .save()

  }

}
