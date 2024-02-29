package org.example.exercices

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, length, lit, round, row_number, sum, trim}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.example.fuf.sparkSessionFactory.SparkSessionFactory


object Ej1_CreatingTablesOnTextFormat {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Creating Tables On Text Format")

    import spark.implicits._

    // 1.2
    val schema = StructType(Array(
      StructField("COD_DISTRITO", IntegerType),
      StructField("DESC_DISTRITO", StringType),
      StructField("COD_DIST_BARRIO", IntegerType),
      StructField("DESC_BARRIO", StringType),
      StructField("COD_BARRIO", IntegerType),
      StructField("COD_DIST_SECCION", IntegerType),
      StructField("COD_SECCION", IntegerType),
      StructField("COD_EDAD_INT", StringType),
      StructField("ESPANOLESHOMBRES", IntegerType),
      StructField("ESPANOLESMUJERES", IntegerType),
      StructField("EXTRANJEROSHOMBRES", IntegerType),
      StructField("EXTRANJEROSMUJERES", IntegerType),
      StructField("FX_CARGA", TimestampType),
      StructField("FX_DATOS_INI", TimestampType),
      StructField("FX_DATOS_FIN", TimestampType)
    ))

    // 1.1
    val datos_padron = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(schema)
      .csv(fileInjected)

    // datos_padron.printSchema()
    // datos_padron.show(5, false)

    // 1.3
    val trimed_dp = datos_padron.withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
      .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))

    // trimed_dp_.show(5, false)
    // val lastFile = datos_padron.limit(5)
    // lastFile.show()



    // Parquet Ej2
    // 2.1 CTAS ==> CREATE TABLE AS ...
    // 2.2 DataFrame (Table) with useless spaces
    val padron_parquet = datos_padron
    // padron_parquet.write.mode("overwrite").parquet("src/main/resources/out/exercises.ej1/reg")
    // padron_parquet.show(5, false)

    // 2.3 DataFrame (Table) without useless spaces (Trimed)
    val trimed_padron_parquet = trimed_dp
    // trimed_padron_parquet.write.mode("overwrite").parquet("src/main/resources/out/exercises.ej1/trimed")
    // trimed_padron_parquet.show(5, false)

    // 2.5 Investiga en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos.
    /*
      Una de sus principales ventajas es su eficiencia en la lectura y la escritura de datos. Al ser un formato columnar, Parquet almacena los datos en columnas en lugar de filas,
      lo que significa que los almacena juntos y no es necesario leer toda la fila para acceder a una sola columna.
     */

    // 2.6
    /*
      regSize = 2396 + 20 KB
      trimedSize = 2369 + 20 KB
     */

    // Impala Ej3
    // 3.1 ¿Qué es Impala?
    /*
      Apache Impala es una herramienta escalable de procesamiento MPP (Massively Parallel Processing). Tiene licencia open source.
      Fue desarrollada inicialmente por Cloudera y más tarde incluida en la Apache Software Foundation. Está incñuida en las distribuciones de Cloudera.
      Sirve para realizar consultas SQL interactivas con muy baja latencia. Soporta Parquet, ORC, Json, Avro, Kudu, Hive, HBase, Amazon S3 o ADLS
     */

    // 3.2 ¿En que se diferencia de Hive?
    /*
      Hive está escrito en Java mientras que Imapala está escrito en C++.
      Se utiliza Hive en ETL processes (Extract, Transform, Load) mientras que Impala está destinado a tareas como la inteligencia empresarial
      Impala ejecuta consultas SQL en tiempo real, mientras que Hive se caracteriza por datos bajos y velocidad de giro.
      Con consultas simples Impala puede ejecutarse hasta 69 veces más rápido, pero Hive maneja mejor las consultas complejas.
      El rendimiento de Hive es significativamente mayor que el de Impala.
      Hive es un sistema tolerante a fallos que preserva todos los medios intermedios mientras que Impala no puede considerarse tolerante a fallos (está ligada a la memoria)
     */

    // 3.3 ¿En qué consiste INVALIDATE METADATA?
    /*
      Marca la METADATA en la tabla como un State y la próxima vez que el servicio de Impala realice una query contra esta tabla con su METADATA invalidada
      Impala recargará la METADATA asiciada antes de proceder con la query. Esto es MUY CARO en recursos
     */


    // Spark Ej6
    // 6.3
    val barriosDistinct = trimed_dp.select("DESC_BARRIO").distinct()
    barriosDistinct.show(131)

    // 6.4
    datos_padron.createOrReplaceTempView("padron")
    val numBarriosDiferentes = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) AS num_barrios_diferentes FROM padron")
    numBarriosDiferentes.show()

    // 6.5
    val datos_padron_longitud1 = trimed_dp.withColumn("longitud", length(col("DESC_DISTRITO")))
    val datos_padron_longitud2 = spark.sql("SELECT *, LENGTH(DESC_DISTRITO) AS longitud FROM padron")
    datos_padron_longitud1.show()
    datos_padron_longitud2.show()

    // 6.6
    val datos_padron_cinco1 = trimed_dp.withColumn("cinco", lit(5))
    val datos_padron_cinco2 = spark.sql("SELECT *, 5 AS cinco FROM padron")
    datos_padron_cinco1.show()
    datos_padron_cinco2.show()

    // 6.7
    val datos_padron_sinCinco = datos_padron_cinco1.drop("cinco")
    datos_padron_sinCinco.show()

    // 6.8
    // trimed_dp.write.mode("overwrite").partitionBy("DESC_DISTRITO").parquet("src/main/resources/out/exercises.ej1/partitionedDistrito")
    // trimed_dp.write.mode("overwrite").partitionBy("DESC_BARRIO").parquet("src/main/resources/out/exercises.ej1/partitionedBarrio") // No funciona

    // 6.9
    //trimed_dp.cache()

    // 6.10
    spark.sql("""SELECT DESC_DISTRITO, DESC_BARRIO, SUM(ESPANOLESHOMBRES) AS TotalEspanolesHombres, SUM(ESPANOLESMUJERES) AS TotalEspanolesMujeres, SUM(EXTRANJEROSHOMBRES) AS TotalExtranjerosHombres, SUM(EXTRANJEROSMUJERES) AS TotalExtranjerosMujeres FROM padron GROUP BY DESC_DISTRITO, DESC_BARRIO ORDER BY TotalExtranjerosMujeres DESC, TotalExtranjerosHombres DESC""").show()

    // 6.11
    //trimed_dp.unpersist()

    // 6.12
    val df = trimed_dp.groupBy("DESC_DISTRITO", "DESC_BARRIO").agg(sum("ESPANOLESHOMBRES").alias("TotalEspañolesHombres"))
    df.show()

    val df_join = trimed_dp.join(df, Seq("DESC_DISTRITO", "DESC_BARRIO"), "inner")
    df_join.show()

    // 6.13
    val windowSpec = Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")
    val df_withSum = trimed_dp.withColumn("TotalEspañolesHombres", sum("ESPANOLESHOMBRES").over(windowSpec))

    val df_join_withSum = trimed_dp.join(df_withSum, Seq("DESC_DISTRITO", "DESC_BARRIO"), "inner")
    df_join_withSum.show()

    // 6.14
    val filtered_df = trimed_dp.filter($"DESC_DISTRITO".isin("CENTRO", "BARAJAS", "RETIRO"))

    val pivoted_df = filtered_df.groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO")
      .agg(sum("ESPANOLESMUJERES").alias("TotalEspañolasMujeres"))
      .orderBy("COD_EDAD_INT")

    pivoted_df.show(2000)

    // 6.15
    val windowSpec2 = Window.partitionBy("COD_EDAD_INT")

    val total_sum_df = pivoted_df.withColumn("TotalEspañolasMujeres",
      sum(col("CENTRO") + col("BARAJAS") + col("RETIRO")).over(windowSpec2))

    val percentage_df = total_sum_df.withColumn("CENTRO_Percent",
      round(col("CENTRO") / col("TotalEspañolasMujeres") * 100, 2)
    ).withColumn("BARAJAS_Percent",
      round(col("BARAJAS") / col("TotalEspañolasMujeres") * 100, 2)
    ).withColumn("RETIRO_Percent",
      round(col("RETIRO") / col("TotalEspañolasMujeres") * 100, 2)
    )
    percentage_df.show()

    // 6.16
    trimed_dp.write.mode("overwrite").partitionBy("DESC_DISTRITO").csv("src/main/resources/out/exercises.ej1/csv/")

    // 6.17
    trimed_dp.write.mode("overwrite").partitionBy("DESC_DISTRITO").parquet("src/main/resources/out/exercises.ej1/parquet/")

    // No me deja particionar por DESC_BARRIO

  }

}
