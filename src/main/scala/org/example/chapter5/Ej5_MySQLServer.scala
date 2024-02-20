package org.example.chapter5

import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory

import java.util.Properties

object Ej5_MySQLServer {

  def run(): Unit = {

    val spark = SparkSessionFactory.createSparkSession("MySQL Server")

    // Loading data from a JDBC source
    // Configure jdbcUrl
    val jdbcURL = "jdbc:sqlserver://[DBSERVER]:1433;databse=[DATABASE]"

    // Create a Properties() object to hold the parameters
    // You can create the JDBC URL without passing the user/password parameters directly
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")
    cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Load data using the connection properties
    val jdbcDF = spark.read.jdbc(jdbcURL, "[TABLENAME]", cxnProp)

    // Saving data to a JDBC source
    jdbcDF.write.jdbc(jdbcURL, "[TABLENAME]", cxnProp)

  }

}
