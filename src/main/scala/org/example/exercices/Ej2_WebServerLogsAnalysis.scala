package org.example.exercices

import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count, length, map_filter, regexp_replace, substring}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej2_WebServerLogsAnalysis {

  def run(aug: String, jul: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Web Server logs Analysis")

    val schema = StructType(Array(
      StructField("Host", StringType),
      StructField("Date", StringType),
      StructField("Request-Method", StringType),
      StructField("Resource", StringType),
      StructField("Protocol", StringType),
      StructField("HTTP-Status", StringType),
      StructField("Size", IntegerType)
    ))

    // TODO LO IMPRESCINDIBLE PARA SEGUIR CON EL EJERCICIO ESTÁ COMENTADO PARA AHORRAR TIEMPO DE EJECUCIÓN

    // val rawAugLogData = spark.read.text(aug)
    // val rawJulLogData = spark.read.text(jul)
    // rawAugLogData.show()
    // rawJulLogData.show()

    // Convierto una única columna value en 6 distintas con todos los campos que me interesan

    /*
    val augLogData = rawAugLogData.select(
      functions.split(col("value"), " ")(0).alias("Host"),
      substring(regexp_replace(functions.split(col("value"), " ")(3), "Aug", "08"), 2, 20).alias("Date"),
      substring(functions.split(col("value"), " ")(5), 2, 1000).alias("Request-Method"),
      functions.split(col("value"), " ")(6).alias("Resource"),
      substring(functions.split(col("value"), " ")(7), 1, 8).alias("Protocol"),
      functions.split(col("value"), " ")(8).alias("HTTP-Status"),
      functions.split(col("value"), " ")(9).alias("Size").cast(IntegerType)
    )
     */
    // augLogData.show()

    /*
    val julLogData = rawJulLogData.select(
      functions.split(col("value"), " ")(0).alias("Host"),
      substring(regexp_replace(functions.split(col("value"), " ")(3), "Jul", "07"), 2, 20).alias("Date"),
      substring(functions.split(col("value"), " ")(5), 2, 5).alias("Request-Method"),
      functions.split(col("value"), " ")(6).alias("Resource"),
      substring(functions.split(col("value"), " ")(7), 1, 8).alias("Protocol"),
      functions.split(col("value"), " ")(8).alias("HTTP-Status"),
      functions.split(col("value"), " ")(9).alias("Size").cast(IntegerType),
    )
     */
    // julLogData.show()

    // Ahora los guardo en formato parquet para cargarlos luego en mis dataFrames "base"

    /*
    val augLogDataCoalesce = augLogData.coalesce(1)
    augLogDataCoalesce.write.mode("overwrite").parquet("src/main/resources/out/exercises.ej2/aug")
    println(augLogDataCoalesce.rdd.getNumPartitions)
    println(augLogData.rdd.getNumPartitions)
    val julLogDataCoalesce = julLogData.coalesce(1)
    julLogDataCoalesce.write.mode("overwrite").parquet("src/main/resources/out/exercises.ej2/jul")
    println(julLogDataCoalesce.rdd.getNumPartitions)
    println(julLogData.rdd.getNumPartitions)
     */

    // val rawParquetAugLogs = spark.read.schema(schema).parquet("src/main/resources/out/exercises.ej2/aug")
    // val rawParquetJulLogs = spark.read.schema(schema).parquet("src/main/resources/out/exercises.ej2/jul")

    // val joined = rawParquetAugLogs.coalesce(1).union(rawParquetJulLogs).coalesce(1)
    // joined.write.mode("overwrite").parquet("src/main/resources/out/exercises.ej2/union")


    val rawParquetLogs = spark.read.schema(schema).parquet("src/main/resources/out/exercises.ej2/union")
    rawParquetLogs.show()

    // ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos
    val protocolCounts = rawParquetLogs
      .filter(col("Protocol").isNotNull)
      .groupBy("Protocol")
      .agg(count("*").alias("Protocol Counting"))
    protocolCounts.show(10000)

    // ¿Cuáles son,los códigos de estado más comunes en la web? Agrúpalos y ordenalos
    val mostCommonHTTPStatus = rawParquetLogs
      .filter(col("HTTP-Status").isNotNull)
      .groupBy("HTTP-Status")
      .agg(count("*").alias("HTTPS-Status Counting"))
      .orderBy(col("HTTPS-Status Counting").desc)
    mostCommonHTTPStatus.show(3)
    // Los más comunes son 200 -> 3092682, 304 -> 266764, 302 -> 73021

    // ¿Y los métodos de petición web (verbos) más utilizados?
    val mostCommonResourceMethod = rawParquetLogs
      .filter(col("Request-Method").isNotNull)
      .groupBy("Request-Method")
      .agg(count("*").alias("Request-Method Counting"))
      .orderBy(col("Request-Method Counting").desc)
    mostCommonResourceMethod.show(3)
    // Los más comunes son GET -> 3453458, HEAD -> 7917, POST -> 222

    // ¿Qué recurso tuvo la mayortransferencia de bytes de la página web?
    val sizedUniqueResource = rawParquetLogs.select("Resource", "Size").filter(col("Size").isNotNull).orderBy(col("Size").desc)
    sizedUniqueResource.show(1)

    // Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.
    val mostVisitedResource = rawParquetLogs
      .filter(col("Resource").isNotNull)
      .groupBy("Resource")
      .agg(count("*").alias("Resource Counting"))
      .orderBy(col("Resource Counting").desc)
    mostVisitedResource.show(1)
    // El recurso con más tráfico recivido es: /images/NASA-logo... -> 208437

    // ¿Qué días la web recibió más tráfico?
    val mostVisitedDay = rawParquetLogs
      .select(substring(col("Date"), 1, 2).alias("Day"), substring(col("Date"), 4, 2).alias("Month"))
      .groupBy("Day", "Month")
      .agg(count("*").alias("Date Counting"))
      .orderBy(col("Date Counting").desc)
    mostVisitedDay.show(1)
    // El día más con más visitas fue el 13 de Julio con 134203 visitas

    // ¿Cuáles son los hosts más comunes?
    val mostCommonHosts = rawParquetLogs
      .filter(col("Host").isNotNull)
      .groupBy("Host")
      .agg(count("*").alias("Host Counting"))
      .orderBy(col("Host Counting").desc)
    mostCommonHosts.show(3)
    // Los más comunes son piweba3y.prodigy.com -> 21988, piweba4y.prodigy.com -> 16437, piweba1y.prodigy.com 12825

    // ¿A qué horas se produce el mayor número de tráfico en la web?
    val mostConcurratedHour = rawParquetLogs
      .select(substring(col("Date"), 12, 8).alias("Hour"))
      .groupBy("Hour")
      .agg(count("*").alias("Hour Counting"))
      .orderBy(col("Hour Counting").desc)
    mostConcurratedHour.show(3)
    // Las horas más comunes son 15:18:23 -> 104, 15:52:16 -> 104, 14:28:00 -> 101 INDEPENDIENTEMENTE DEL MES

    // ¿Cuál es el número de errores 404 que ha habido cada día
    val error404PerDay = rawParquetLogs
      .filter(col("HTTP-Status") === 404)
      .select(substring(col("Date"), 1, 2).alias("Day"))
      .groupBy("Day")
      .agg(count("*").alias("404 Daily Count"))
      //.orderBy(col("404 Daily Count").desc)
      .orderBy(col("Day").asc)
    error404PerDay.show(31)
    // Aquí están ordenados del 01 al 31 del mes, si se quieren ordenar por cantidad de errores descomentar el .orderBy() anterior al descomentado y comentar el descomentado

  }
}
