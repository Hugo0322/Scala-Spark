package org.example.theoryExamples.chapter5

import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej1_UserDefinedFunctions {

  def run(): Unit = {

    val spark = SparkSessionFactory.createSparkSession("User-Defined Functions")

    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    }

    // Register UDF
    spark.udf.register("cubed", cubed)

    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    // Showing the temp view with spark SQL
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    // Evaluation order and null checking in Spark SQL
    // spark.sql("SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1")

    


  }

}
