package org.example.chapter5

import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory

object Ej2_PostgreSQL {

  // Each of the [] should be changed for the actual names

  def run(): Unit = {

    val spark = SparkSessionFactory.createSparkSession("PostgreSQL")

    // Read Option 1: Loading data from a JDBC source using load method
    val jdbcDF1 = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

    // Read Option 2: Loading data form a JDBC source using jdbc method
    // Create connection Properties
    import java.util.Properties
    val cxnProp = new Properties()
    cxnProp.put("user", "[USERNAME]")
    cxnProp.put("password", "[PASSWORD]")
    // Load data using the connection properties
    val jdbcDF2 = spark.read.jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)


    // Write Option 1: Saving data to a JDBC source using save method
    jdbcDF1.write.format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()

    // Write Option 2: Saving data to a JDBC source using JDBC method
    jdbcDF2.write.jdbc(s"jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)
  }

}
