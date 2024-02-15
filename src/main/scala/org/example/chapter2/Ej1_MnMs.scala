package org.example.chapter2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, desc}
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory

object Ej1_MnMs {


  // Función que encapsula toda la lógica del programa
  def run(mnmFile: String): Unit = {
    // Creating SparkSession

    val spark = SparkSessionFactory.createSparkSession("MnMCounter")

    // Allowing spark to printout all more than 20 lines at once
    // spark.conf.set("spark.sql.repl.eagerEval.enabled", true) // No me funciona, no se si debería o lo he mirado mal en google

    // Read the file into a Spark DataFrame using the CSV format
    val mnmDF = spark.read.format("CSV")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Aggregate counts of all colors and group by State and Color
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Show the results for all the States and Colors
    //countMnMDF.show(truncate = false) // Esto solo muestra las 20 primeras filas
    //println(s"Total rows = ${countMnMDF.count()}")

    // Esto si que muestra todas las filas con el formato deseado
    //countMnMDF.show(countMnMDF.count().toInt, truncate = false)

    // Acabo de ver que está el ejecricio resuelto en Scala y lo único necesario es
    // countMnMDF.show(60) // Si realmente sabes el número de filas que van a ser o
    countMnMDF.show(countMnMDF.count().toInt) // para sacar directamente el número exacto de filas que va a tener la tabla¡

    // Esto muestra todas las filas de la "consulta" pero un poco menos formateadas
    // val allRows = countMnMDF.collect()
    // allRows.foreach(println)

    // Filter only California (CA) State and aggregate the counts for each Color
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Show the results for California State
    caCountMnMDF.show(truncate = false)
    println(s"Total rows = ${caCountMnMDF.count()}")

    //Stop the SparkSession
    spark.stop()

  }
}